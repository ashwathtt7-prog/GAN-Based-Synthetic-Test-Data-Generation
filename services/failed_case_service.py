import json
import uuid
from collections import defaultdict, deque

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from config.config import get_data_source, load_config
from ingestion.querylog_miner import QueryLogMiner
from ingestion.sqlglot_parser import DDLParser
from synthesis.masking_engine import MaskingEngine


def _json_safe(value):
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _is_internal_table(table_name: str) -> bool:
    return str(table_name).startswith("_")


class FailedCaseScenarioService:
    def __init__(self, db_client, config: dict | None = None):
        self.db_client = db_client
        self.config = config or load_config()

    def _get_engine(self, source_name: str):
        source = get_data_source(source_name, self.config)
        return create_engine(source["connection_string"]), source["name"]

    def _get_relationships(self, source_name: str) -> list:
        source = get_data_source(source_name, self.config)
        inspector = inspect(create_engine(source["connection_string"]))
        table_names = {
            t.upper() for t in inspector.get_table_names() if not _is_internal_table(t)
        }

        ddl_dir = self.config.get("ingestion", {}).get("ddl_directory", "datasets/ddl")
        query_dir = self.config.get("ingestion", {}).get("query_log_directory", "datasets/query_logs")
        relationships = DDLParser(ddl_dir).parse_relationships() + QueryLogMiner(query_dir).mine_relationships()
        deduped = {}
        for rel in relationships:
            if rel.source_table.upper() not in table_names or rel.target_table.upper() not in table_names:
                continue
            key = (
                rel.source_table.upper(),
                rel.source_column.upper(),
                rel.target_table.upper(),
                rel.target_column.upper(),
            )
            deduped[key] = rel
        return list(deduped.values())

    def _infer_id_column(self, engine, table_name: str) -> str | None:
        candidates = self._candidate_id_columns(engine, table_name)
        return candidates[0] if candidates else None

    def _candidate_id_columns(self, engine, table_name: str) -> list[str]:
        inspector = inspect(engine)
        candidates: list[str] = []

        try:
            pk = inspector.get_pk_constraint(table_name) or {}
            cols = pk.get("constrained_columns") or []
            if cols:
                candidates.extend(cols)
        except Exception:
            candidates = []

        try:
            columns = [col["name"] for col in inspector.get_columns(table_name)]
        except Exception:
            return candidates

        business_suffixes = ("_NO", "_NUMBER", "_NUM", "_REF")
        for suffix in business_suffixes:
            for column in columns:
                upper = column.upper()
                if upper.endswith(suffix) and column not in candidates:
                    candidates.append(column)

        for token in ("ORDER", "INVC", "ACCT", "SUBSCR", "PLAN"):
            for column in columns:
                upper = column.upper()
                parts = [part for part in upper.split("_") if part]
                if token in upper and column not in candidates and any(
                    part in {"NO", "NUMBER", "REF", "NUM"} for part in parts
                ):
                    candidates.append(column)

        if not candidates:
            for column in columns:
                upper = column.upper()
                if upper.endswith("_ID") and column not in candidates:
                    candidates.append(column)

        if not candidates and columns:
            candidates.append(columns[0])
        return candidates

    def list_traceable_tables(self, source_name: str) -> list[dict]:
        engine, resolved_source = self._get_engine(source_name)
        inspector = inspect(engine)
        options = []
        for table_name in inspector.get_table_names():
            if _is_internal_table(table_name):
                continue
            id_columns = self._candidate_id_columns(engine, table_name)
            if not id_columns:
                continue
            id_column = id_columns[0]
            label = f"{table_name} ({id_column})"
            options.append({
                "table_name": table_name,
                "label": label,
                "id_column": id_column,
                "id_columns": id_columns,
            })
        options.sort(key=lambda item: (0 if "ORD" in item["table_name"].upper() else 1, item["table_name"]))
        return {"source_name": resolved_source, "tables": options}

    def list_id_values(self, source_name: str, table_name: str, id_column: str, search: str | None = None, limit: int = 50) -> dict:
        engine, resolved_source = self._get_engine(source_name)
        sql = f'SELECT DISTINCT "{id_column}" AS value FROM "{table_name}"'
        params = {}
        if search:
            sql += f' WHERE CAST("{id_column}" AS VARCHAR) LIKE :pattern'
            params["pattern"] = f"%{search}%"
        sql += f' ORDER BY CAST("{id_column}" AS VARCHAR) LIMIT {int(limit)}'
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        values = [{"value": str(row[0]), "label": str(row[0])} for row in rows if row[0] is not None]
        return {
            "source_name": resolved_source,
            "table_name": table_name,
            "id_column": id_column,
            "values": values,
        }

    def _fetch_rows(self, engine, table_name: str, column_name: str, value, limit: int = 50) -> list[dict]:
        sql = f'SELECT * FROM "{table_name}" WHERE "{column_name}" = :value LIMIT {int(limit)}'
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {"value": value}).fetchall()
        result = []
        for row in rows:
            mapping = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
            result.append({key: _json_safe(val) for key, val in mapping.items()})
        return result

    def trace_case(self, source_name: str, table_name: str, id_column: str, id_value: str, limit_per_link: int = 25) -> dict:
        engine, resolved_source = self._get_engine(source_name)
        relationships = self._get_relationships(resolved_source)

        root_rows = self._fetch_rows(engine, table_name, id_column, id_value, limit=5)
        if not root_rows:
            raise ValueError(f"No rows found for {table_name}.{id_column} = {id_value}")

        tables = defaultdict(list)
        links = []
        seen_row_keys = set()
        queue = deque([(table_name.upper(), row, "root", 0) for row in root_rows])

        while queue:
            current_table, row, mode, depth = queue.popleft()
            row_key = (
                current_table,
                mode,
                json.dumps(row, sort_keys=True, default=str),
            )
            if row_key in seen_row_keys:
                continue
            seen_row_keys.add(row_key)
            tables[current_table].append(row)

            for rel in relationships:
                source_table = rel.source_table.upper()
                target_table = rel.target_table.upper()
                source_col = rel.source_column
                target_col = rel.target_column

                allow_upstream = mode in {"root", "upstream", "downstream"}
                allow_downstream = mode in {"root", "downstream"}

                if allow_upstream and current_table == source_table:
                    parent_value = row.get(source_col)
                    if parent_value is None:
                        continue
                    related_rows = self._fetch_rows(engine, target_table, target_col, parent_value, limit=limit_per_link)
                    if related_rows:
                        links.append({
                            "from_table": current_table,
                            "to_table": target_table,
                            "via_source_column": source_col,
                            "via_target_column": target_col,
                            "match_value": _json_safe(parent_value),
                            "direction": "upstream",
                            "row_count": len(related_rows),
                        })
                        for related_row in related_rows:
                            next_mode = "upstream" if mode in {"root", "upstream"} else "context"
                            if depth < 3:
                                queue.append((target_table, related_row, next_mode, depth + 1))

                if allow_downstream and current_table == target_table:
                    child_value = row.get(target_col)
                    if child_value is None:
                        continue
                    related_rows = self._fetch_rows(engine, source_table, source_col, child_value, limit=limit_per_link)
                    if related_rows:
                        links.append({
                            "from_table": current_table,
                            "to_table": source_table,
                            "via_source_column": target_col,
                            "via_target_column": source_col,
                            "match_value": _json_safe(child_value),
                            "direction": "downstream",
                            "row_count": len(related_rows),
                        })
                        for related_row in related_rows:
                            if depth < 3:
                                queue.append((source_table, related_row, "downstream", depth + 1))

        payload_tables = []
        for name, rows in sorted(tables.items()):
            unique_rows = []
            seen = set()
            for row in rows:
                encoded = json.dumps(row, sort_keys=True, default=str)
                if encoded in seen:
                    continue
                seen.add(encoded)
                unique_rows.append(row)
            payload_tables.append(
                {
                    "table_name": name,
                    "row_count": len(unique_rows),
                    "rows": unique_rows,
                }
            )

        return {
            "source_name": resolved_source,
            "root": {
                "table_name": table_name.upper(),
                "id_column": id_column,
                "id_value": id_value,
                "rows": root_rows,
            },
            "tables": payload_tables,
            "links": links,
        }

    def _mask_dataframe_consistently(self, df: pd.DataFrame, policies: list) -> pd.DataFrame:
        masking_engine = MaskingEngine(
            self.config.get("generation", {}).get("faker_locale", "en_US")
        )
        result = df.copy()
        policy_map = {policy.column_name: policy for policy in policies if hasattr(policy, "column_name")}

        for column in list(result.columns):
            policy = policy_map.get(column)
            if not policy:
                continue

            strategy = getattr(policy, "masking_strategy", "passthrough")
            if strategy == "passthrough":
                continue
            if strategy == "suppress":
                result = result.drop(columns=[column])
                continue

            source_series = result[column]
            unique_values = pd.Series(source_series.dropna().unique())
            if unique_values.empty:
                continue

            if strategy == "substitute_realistic":
                masked_uniques = masking_engine._substitute_realistic(unique_values, column, policy)
            elif strategy == "format_preserving":
                masked_uniques = masking_engine._format_preserving(unique_values, column, policy)
            elif strategy == "generalise":
                masked_uniques = masking_engine._generalise(unique_values, column, policy)
            else:
                continue

            mapping = {
                original: _json_safe(masked)
                for original, masked in zip(unique_values.tolist(), masked_uniques.tolist())
            }
            result[column] = source_series.map(lambda value: mapping.get(value) if pd.notna(value) else None)

        return result

    def generate_synthetic_case(self, source_name: str, table_name: str, id_column: str, id_value: str) -> dict:
        trace = self.trace_case(source_name, table_name, id_column, id_value)

        synthetic_tables = []
        for table_entry in trace["tables"]:
            table_name_entry = table_entry["table_name"]
            rows = table_entry["rows"]
            frame = pd.DataFrame(rows)
            with self.db_client.session() as session:
                policies = self.db_client.get_column_policies(
                    session,
                    table_name=table_name_entry,
                    source_name=trace["source_name"],
                )
            synthetic_frame = self._mask_dataframe_consistently(frame, policies)
            synthetic_tables.append(
                {
                    "table_name": table_name_entry,
                    "row_count": len(synthetic_frame),
                    "columns": list(synthetic_frame.columns),
                    "rows": synthetic_frame.to_dict(orient="records"),
                }
            )

        scenario_id = str(uuid.uuid4())
        payload = {
            "scenario_id": scenario_id,
            "source_name": trace["source_name"],
            "root": trace["root"],
            "tables": synthetic_tables,
            "links": trace["links"],
        }

        with self.db_client.session() as session:
            self.db_client.upsert_failed_case_scenario(
                session,
                {
                    "scenario_id": scenario_id,
                    "source_name": trace["source_name"],
                    "root_table": trace["root"]["table_name"],
                    "id_column": id_column,
                    "id_value": str(id_value),
                    "display_label": f'{trace["root"]["table_name"]}:{id_value}',
                    "trace_payload": trace,
                    "synthetic_payload": payload,
                },
            )

        return payload
