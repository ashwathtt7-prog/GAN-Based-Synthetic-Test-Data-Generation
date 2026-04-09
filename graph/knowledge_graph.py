"""
In-Memory Knowledge Graph backed by NetworkX.

Replaces Neo4j server dependency while providing identical functionality:
- Table/Column/Relationship nodes and edges
- Abbreviation dictionary
- Domain partitioning via Louvain
- All 5 LLM graph tools (get_table_schema, get_relationships, etc.)

Zero external services required. Works on any system with Python + networkx.
"""

import json
import logging
import yaml
import networkx as nx
from pathlib import Path
from threading import Lock
from typing import Optional

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Singleton graph instance shared across modules
# ──────────────────────────────────────────────
_graph_instance: Optional["KnowledgeGraph"] = None
_lock = Lock()


def get_knowledge_graph() -> "KnowledgeGraph":
    """Get or create the singleton KnowledgeGraph."""
    global _graph_instance
    with _lock:
        if _graph_instance is None:
            _graph_instance = KnowledgeGraph()
        return _graph_instance


class KnowledgeGraph:
    """
    NetworkX-backed knowledge graph that mirrors the Neo4j schema:

    Nodes:
      - Table nodes:  {type: 'table', name, row_count, column_count, domain}
      - Column nodes: {type: 'column', name, table, data_type, null_rate, unique_count, top_values, ...}

    Edges:
      - HAS_COLUMN:  Table → Column
      - RELATES_TO:  Table → Table (FK relationships)

    Also stores:
      - abbreviation_dict: global telecom abbreviation dictionary
    """

    def __init__(self):
        self.G = nx.DiGraph()
        self.abbreviation_dict: dict[str, str] = {}
        self._seed_abbreviations()
        logger.info("KnowledgeGraph initialized (NetworkX in-memory)")

    # ──────────────────────────────────────────
    # Construction
    # ──────────────────────────────────────────

    def _seed_abbreviations(self):
        """Load abbreviation dictionary from config.yaml."""
        config_path = Path(__file__).parent.parent / "config" / "config.yaml"
        if config_path.exists():
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            self.abbreviation_dict = config.get("abbreviations", {})
            logger.info(f"Seeded {len(self.abbreviation_dict)} abbreviations into knowledge graph")

    def add_abbreviation(self, token: str, expansion: str):
        """Add or update an abbreviation in the dictionary."""
        self.abbreviation_dict[token.upper()] = expansion

    def build_graph(self, tables, relationships):
        """
        Build the full knowledge graph from ingested schema metadata.

        Args:
            tables: list of TableMetadata pydantic models
            relationships: list of RelationshipInfo pydantic models
        """
        # Add table nodes
        for table in tables:
            table_id = f"table:{table.table_name}"
            self.G.add_node(table_id,
                            node_type="table",
                            name=table.table_name,
                            row_count=table.row_count,
                            column_count=table.column_count,
                            domain=None)

            # Add column nodes and HAS_COLUMN edges
            for col in table.columns:
                col_id = f"col:{table.table_name}.{col.column_name}"
                self.G.add_node(col_id,
                                node_type="column",
                                name=col.column_name,
                                table=table.table_name,
                                data_type=col.data_type,
                                row_count=col.row_count,
                                null_rate=col.null_rate,
                                unique_count=col.unique_count,
                                top_values=col.top_values if col.top_values else [])

                self.G.add_edge(table_id, col_id, edge_type="HAS_COLUMN")

        # Add FK relationship edges (Table → Table)
        for rel in relationships:
            src_id = f"table:{rel.source_table}"
            tgt_id = f"table:{rel.target_table}"
            # Ensure both nodes exist
            if src_id in self.G and tgt_id in self.G:
                self.G.add_edge(src_id, tgt_id,
                                edge_type="RELATES_TO",
                                source_column=rel.source_column,
                                target_column=rel.target_column,
                                relationship_type=rel.relationship_type,
                                confidence=rel.confidence)

        logger.info(f"Knowledge graph built: {self._count_tables()} tables, "
                     f"{self._count_columns()} columns, "
                     f"{self._count_relationships()} relationships")

    def set_domain(self, table_name: str, domain: str):
        """Write domain assignment back to the graph."""
        table_id = f"table:{table_name}"
        if table_id in self.G:
            self.G.nodes[table_id]["domain"] = domain

    def update_column_policy(self, table_name: str, column_name: str, policy_data: dict):
        """Write LLM classification results back to the column node."""
        col_id = f"col:{table_name}.{column_name}"
        if col_id in self.G:
            for k, v in policy_data.items():
                self.G.nodes[col_id][k] = v

    # ──────────────────────────────────────────
    # LLM Graph Tool implementations
    # (same signatures as the Neo4j graph_tools)
    # ──────────────────────────────────────────

    def get_table_schema(self, table_name: str) -> str:
        """Returns schema and statistical profile of all columns in a table."""
        table_name = table_name.upper()
        table_id = f"table:{table_name}"

        if table_id not in self.G:
            return f"Table {table_name} not found in knowledge graph."

        table_data = dict(self.G.nodes[table_id])
        table_data.pop("node_type", None)

        columns = []
        for _, col_id, edge_data in self.G.out_edges(table_id, data=True):
            if edge_data.get("edge_type") == "HAS_COLUMN":
                col_data = dict(self.G.nodes[col_id])
                col_data.pop("node_type", None)
                columns.append(col_data)

        return json.dumps({"table": table_data, "columns": columns}, indent=2, default=str)

    def get_relationships(self, table_name: str) -> str:
        """Returns all tables that reference or are referenced by this table."""
        table_name = table_name.upper()
        table_id = f"table:{table_name}"
        rels = []

        # Outgoing RELATES_TO
        for _, tgt, data in self.G.out_edges(table_id, data=True):
            if data.get("edge_type") == "RELATES_TO":
                other_name = self.G.nodes[tgt].get("name", tgt)
                rels.append({"related_table": other_name, "direction": "outgoing",
                             "details": {k: v for k, v in data.items() if k != "edge_type"}})

        # Incoming RELATES_TO
        for src, _, data in self.G.in_edges(table_id, data=True):
            if data.get("edge_type") == "RELATES_TO":
                other_name = self.G.nodes[src].get("name", src)
                rels.append({"related_table": other_name, "direction": "incoming",
                             "details": {k: v for k, v in data.items() if k != "edge_type"}})

        return json.dumps(rels, indent=2, default=str)

    def get_downstream_tables(self, table_name: str) -> str:
        """Returns all tables downstream (child tables) up to 3 hops via RELATES_TO."""
        table_name = table_name.upper()
        table_id = f"table:{table_name}"
        downstream = []

        visited = set()
        queue = [(table_id, 0)]
        while queue:
            node, depth = queue.pop(0)
            if depth > 3:
                continue
            for _, tgt, data in self.G.out_edges(node, data=True):
                if data.get("edge_type") == "RELATES_TO" and tgt not in visited:
                    visited.add(tgt)
                    tgt_name = self.G.nodes[tgt].get("name", tgt)
                    downstream.append(tgt_name)
                    queue.append((tgt, depth + 1))

        return json.dumps(downstream, indent=2)

    def get_abbreviation(self, token: str) -> str:
        """Returns the expanded form of an abbreviation from the dictionary."""
        expansion = self.abbreviation_dict.get(token.upper())
        return expansion if expansion else "null"

    def get_domain(self, table_name: str) -> str:
        """Returns the business domain assigned to the table."""
        table_name = table_name.upper()
        table_id = f"table:{table_name}"
        if table_id in self.G:
            domain = self.G.nodes[table_id].get("domain")
            return domain if domain else "unknown"
        return "unknown"

    # ──────────────────────────────────────────
    # Domain Partitioning (Louvain)
    # ──────────────────────────────────────────

    def partition_domains(self) -> dict[str, str]:
        """
        Run Louvain community detection on the table-level graph.
        Returns {table_name: domain_name} mapping.
        """
        from community import community_louvain

        # Build undirected table-only subgraph for clustering
        table_graph = nx.Graph()
        for node, data in self.G.nodes(data=True):
            if data.get("node_type") == "table":
                table_graph.add_node(data["name"])

        for src, tgt, data in self.G.edges(data=True):
            if data.get("edge_type") == "RELATES_TO":
                src_name = self.G.nodes[src].get("name")
                tgt_name = self.G.nodes[tgt].get("name")
                if src_name and tgt_name:
                    table_graph.add_edge(src_name, tgt_name)

        if len(table_graph.nodes) == 0:
            return {}

        # Run Louvain
        partition = community_louvain.best_partition(table_graph)

        # Group by cluster
        clusters: dict[int, list[str]] = {}
        for node, cluster_id in partition.items():
            clusters.setdefault(cluster_id, []).append(node)

        # Semantic naming based on table name patterns
        domain_map = {}
        for cid, nodes in clusters.items():
            if any("CUST" in n or "SUBSCR" in n or "SVC" in n for n in nodes):
                domain_name = "customer_management"
            elif any("BLNG" in n or "INVC" in n or "PYMT" in n for n in nodes):
                domain_name = "billing_revenue"
            elif any("NTWK" in n or "CELL" in n for n in nodes):
                domain_name = "network_operations"
            else:
                domain_name = f"domain_{cid}"

            for node in nodes:
                domain_map[node] = domain_name

        # Apply manual overrides from config
        overrides_path = Path(__file__).parent.parent / "config" / "domain_overrides.yaml"
        if overrides_path.exists():
            with open(overrides_path, "r") as f:
                cfg = yaml.safe_load(f) or {}
            for table, override_domain in cfg.get("overrides", {}).items():
                domain_map[table] = override_domain
                logger.info(f"Applied domain override: {table} -> {override_domain}")

        # Write domains back to graph nodes
        for table_name, domain in domain_map.items():
            self.set_domain(table_name, domain)

        logger.info(f"Domain partitioning complete: {len(set(domain_map.values()))} domains assigned")
        return domain_map

    # ──────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────

    def _count_tables(self) -> int:
        return sum(1 for _, d in self.G.nodes(data=True) if d.get("node_type") == "table")

    def _count_columns(self) -> int:
        return sum(1 for _, d in self.G.nodes(data=True) if d.get("node_type") == "column")

    def _count_relationships(self) -> int:
        return sum(1 for _, _, d in self.G.edges(data=True) if d.get("edge_type") == "RELATES_TO")

    def get_all_table_names(self) -> list[str]:
        return [d["name"] for _, d in self.G.nodes(data=True) if d.get("node_type") == "table"]

    def get_table_relationships_raw(self) -> list[dict]:
        """Return raw relationship data for topological sorting etc."""
        rels = []
        for src, tgt, data in self.G.edges(data=True):
            if data.get("edge_type") == "RELATES_TO":
                rels.append({
                    "source_table": self.G.nodes[src].get("name"),
                    "target_table": self.G.nodes[tgt].get("name"),
                    "source_column": data.get("source_column"),
                    "target_column": data.get("target_column"),
                })
        return rels
