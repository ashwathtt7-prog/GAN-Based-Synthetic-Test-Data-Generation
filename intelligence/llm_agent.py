"""
LLM Agent
LangChain tool-calling agent to execute semantic reasoning for column classification.
"""

import json
import logging
from langchain_core.messages import SystemMessage, HumanMessage

from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from llm.model_client import get_model_client
from models.schemas import ColumnPolicySchema
from graph.graph_tools import (
    get_table_schema, get_relationships, get_downstream_tables,
    get_abbreviation, get_domain
)

logger = logging.getLogger(__name__)

class LLMAgent:
    def __init__(self):
        self.model_client = get_model_client()
        # In a full LangChain setup, we'd bind tools to the LLM. 
        # For this POC, utilizing our robust ModelClient abstraction:
        
    def classify_column(self, 
                        table_name: str, 
                        column_name_raw: str,
                        column_name_expanded: str,
                        data_type: str,
                        statistical_profile: str,
                        top_values: str,
                        presidio_result: str,
                        abbreviation_status: str) -> ColumnPolicySchema:
        """Execute reasoning for a single column."""
        
        # We simulate the prompt that gives the LLM context it would normally 
        # gather via tool-calling. In a full production loop, the LLM uses ReAct 
        # to call `get_table_schema` etc. Here we pass the tools context instruction.
        
        prompt = f"""
Reason about the following database column and produce a ColumnPolicySchema JSON.

Table: {table_name}
Column: {column_name_expanded} (original: {column_name_raw})
Data type: {data_type}
Statistical profile: {statistical_profile}
Top values with frequencies: {top_values}
Presidio result: {presidio_result}
Abbreviation resolution status: {abbreviation_status}

Analyze what this column means in a telecom business context. 
Determine if it holds sensitive business data (revenue, margins, risk scores).
Define constraints required to generate realistic synthetic data.
Decide on an appropriate deduplication mode for this table's synthetic generation.
        """
        
        try:
             # Invoke our LLM client which enforces Pydantic structured output
             policy = self.model_client.invoke(
                 prompt=prompt,
                 output_schema=ColumnPolicySchema,
                 retry_on_failure=True
             )
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
