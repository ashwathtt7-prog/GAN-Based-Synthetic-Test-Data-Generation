"""
LLM Agent — Semantic Reasoning for Column Classification.

Key behavior per master_prompt.md:
  The LLM traverses the Neo4j knowledge graph (now NetworkX-backed) to gather
  context BEFORE making classification decisions. It calls graph tools to:
    1. get_table_schema — see sibling columns for context
    2. get_relationships — understand FK relationships
    3. get_downstream_tables — see impact radius
    4. get_abbreviation — expand telecom abbreviations
    5. get_domain — know which business domain this table belongs to

  The gathered context is injected into the classification prompt so the LLM
  can make informed decisions rather than guessing from column name alone.
"""

import json
import logging

from llm.model_client import get_model_client
from models.schemas import ColumnPolicySchema
from graph.knowledge_graph import get_knowledge_graph

logger = logging.getLogger(__name__)


class LLMAgent:
    def __init__(self):
        self.model_client = get_model_client()
        self.kg = get_knowledge_graph()

    def _gather_graph_context(self, table_name: str, column_name_raw: str) -> dict:
        """
        Traverse the knowledge graph to gather rich context before LLM classification.
        This is the key integration — the LLM doesn't guess in isolation,
        it reasons over the full graph context.
        """
        context = {}

        # 1. Get full table schema (sibling columns give context)
        try:
            context["table_schema"] = self.kg.get_table_schema(table_name)
        except Exception as e:
            logger.debug(f"get_table_schema failed for {table_name}: {e}")
            context["table_schema"] = "unavailable"

        # 2. Get FK relationships (understand what this table connects to)
        try:
            context["relationships"] = self.kg.get_relationships(table_name)
        except Exception as e:
            logger.debug(f"get_relationships failed for {table_name}: {e}")
            context["relationships"] = "[]"

        # 3. Get downstream tables (impact radius — who depends on this table)
        try:
            context["downstream_tables"] = self.kg.get_downstream_tables(table_name)
        except Exception as e:
            logger.debug(f"get_downstream_tables failed for {table_name}: {e}")
            context["downstream_tables"] = "[]"

        # 4. Resolve abbreviations in column name tokens
        tokens = column_name_raw.split('_')
        expansions = {}
        for token in tokens:
            try:
                exp = self.kg.get_abbreviation(token)
                if exp != "null":
                    expansions[token] = exp
            except Exception:
                pass
        context["abbreviation_expansions"] = expansions

        # 5. Get the business domain for this table
        try:
            context["domain"] = self.kg.get_domain(table_name)
        except Exception as e:
            context["domain"] = "unknown"

        return context

    def classify_column(self,
                        table_name: str,
                        column_name_raw: str,
                        column_name_expanded: str,
                        data_type: str,
                        statistical_profile: str,
                        top_values: str,
                        presidio_result: str,
                        abbreviation_status: str) -> ColumnPolicySchema:
        """
        Execute reasoning for a single column using graph context.

        Flow:
          1. Traverse knowledge graph to gather context
          2. Build rich prompt with graph context
          3. Call LLM with Pydantic schema enforcement
          4. Write classification back to graph
        """

        # Step 1: Gather context from knowledge graph
        graph_context = self._gather_graph_context(table_name, column_name_raw)

        # Step 2: Build context-enriched prompt
        prompt = f"""
Reason about the following database column and produce a ColumnPolicySchema JSON.

## Column Under Analysis
Table: {table_name}
Column: {column_name_expanded} (original: {column_name_raw})
Data type: {data_type}
Statistical profile: {statistical_profile}
Top values with frequencies: {top_values}
Presidio PII scan result: {presidio_result}
Abbreviation resolution status: {abbreviation_status}

## Knowledge Graph Context (gathered from graph traversal)

### Business Domain
This table belongs to domain: {graph_context['domain']}

### Abbreviation Dictionary Matches
{json.dumps(graph_context['abbreviation_expansions'], indent=2) if graph_context['abbreviation_expansions'] else 'No abbreviation matches found for column tokens.'}

### Table Schema (sibling columns for context)
{graph_context['table_schema']}

### Foreign Key Relationships
{graph_context['relationships']}

### Downstream Tables (tables that depend on this table, up to 3 hops)
{graph_context['downstream_tables']}

## Instructions
1. Use the knowledge graph context above to understand what this column means in the telecom business.
2. The sibling columns and FK relationships reveal the table's role — use them.
3. If abbreviation expansions are provided, use them to understand the column name.
4. Determine if it holds PII, sensitive business data (revenue, margins, risk scores), or is safe.
5. Define constraints required to generate realistic synthetic data.
6. Decide masking strategy: passthrough (safe), substitute_realistic (PII), format_preserving, suppress, or generalise.
7. Choose dedup mode: entity (unique per record), reference (skip dedup), or event (FK repeats OK).
8. Set business_importance: critical, important, or low.
9. Flag edge cases that synthetic generation should test.
"""

        try:
            policy = self.model_client.invoke(
                prompt=prompt,
                output_schema=ColumnPolicySchema,
                retry_on_failure=True
            )

            # Step 4: Write classification back to the knowledge graph
            self.kg.update_column_policy(table_name, column_name_raw, {
                "pii_classification": policy.pii_classification,
                "masking_strategy": policy.masking_strategy,
                "business_importance": policy.business_importance,
                "llm_confidence": policy.llm_confidence,
                "dedup_mode": policy.dedup_mode,
            })

            return policy

        except Exception as e:
            logger.error(f"LLM Classification failed for {table_name}.{column_name_raw}: {e}")
            # Fallback policy for robust pipeline execution
            return ColumnPolicySchema(
                column_name=column_name_raw,
                table_name=table_name,
                pii_classification="uncertain",
                sensitivity_reason=f"LLM Classification Failed: {str(e)[:100]}",
                masking_strategy="passthrough",
                constraint_profile={},
                business_importance="low",
                edge_case_flags=[],
                dedup_mode="reference",
                llm_confidence=0.0,
                abbreviation_resolved=False,
                notes="Created by automated fallback"
            )
