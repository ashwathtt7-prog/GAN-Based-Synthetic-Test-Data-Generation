"""
Pipeline Orchestrator
Master coordinator that gloms together all 4 layers.
"""

import logging
import uuid
from datetime import datetime
from config.config import load_config

# Import layers
from ingestion.schema_connector import SchemaConnector
from ingestion.sqlglot_parser import DDLParser
from ingestion.querylog_miner import QueryLogMiner
from graph.neo4j_builder import Neo4jBuilder
from graph.domain_partitioner import DomainPartitioner
from intelligence.presidio_scanner import PresidioScanner
from intelligence.abbreviation_resolver import AbbreviationResolver
from intelligence.llm_agent import LLMAgent
from intelligence.strategy_planner import StrategyPlanner
from synthesis.ctgan_model import CTGANModel

from db.client import DatabaseClient
import db.schema as db_models

logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self):
        self.config = load_config()
        self.db_client = DatabaseClient()
        self.run_id = None
        
    def initialize_run(self) -> str:
        self.run_id = str(uuid.uuid4())
        with self.db_client.get_session() as session:
             run = db_models.PipelineRun(
                 run_id=self.run_id,
                 status="initialized",
                 current_step="Starting"
             )
             session.add(run)
             session.commit()
        return self.run_id
        
    def _update_status(self, step: str, progress: float, status: str = "running"):
         with self.db_client.get_session() as session:
             run = session.query(db_models.PipelineRun).filter_by(run_id=self.run_id).first()
             if run:
                 run.current_step = step
                 run.progress_pct = progress
                 run.status = status
                 session.commit()
        
    def execute_pipeline(self, run_id: str):
        self.run_id = run_id
        try:
             # --- Phase 1: Ingestion ---
             self._update_status("Schema Ingestion", 5.0)
             connector = SchemaConnector("sqlite:///datasets/telecom_source.db")
             tables = connector.extract_schema()
             
             ddl_parser = DDLParser("datasets/ddl")
             explicit_rels = ddl_parser.parse_relationships()
             
             miner = QueryLogMiner("datasets/query_logs")
             implicit_rels = miner.mine_relationships()
             
             all_rels = explicit_rels + implicit_rels
             
             # --- Phase 2: Graph Build ---
             self._update_status("Graph Construction", 15.0)
             nc = self.config['neo4j']
             builder = Neo4jBuilder(nc['uri'], nc['username'], nc['password'])
             builder.build_graph(tables, all_rels)
             builder.close()
             
             # --- Phase 3: Domain Partitioning ---
             self._update_status("Domain Partitioning", 25.0)
             partitioner = DomainPartitioner(nc['uri'], nc['username'], nc['password'])
             domain_map = partitioner.partition_domains()
             
             # Cache tables in local memory
             with self.db_client.get_session() as session:
                 for t in tables:
                     # Upsert
                     t_meta = session.query(db_models.TableMetadata).filter_by(table_name=t.table_name).first()
                     if not t_meta:
                         t_meta = db_models.TableMetadata(table_name=t.table_name)
                         session.add(t_meta)
                     t_meta.row_count = t.row_count
                     t_meta.column_count = t.column_count
                     t_meta.domain = domain_map.get(t.table_name)
                 session.commit()
                 
             # --- Phase 4: Intelligence (LLM + Presidio) ---
             self._update_status("Intelligence & Semantic Reasoning", 40.0)
             presidio = PresidioScanner(self.config)
             abbrev = AbbreviationResolver()
             agent = LLMAgent()
             planner = StrategyPlanner()
             
             total_tables = len(tables)
             for i, table in enumerate(tables):
                  self._update_status(f"Classifying {table.table_name}", 40.0 + (i/total_tables * 30.0))
                  
                  table_policies = []
                  for col in table.columns:
                       # 1. Expand Abbrev
                       exp_name, fully_res = abbrev.resolve_column_name(col.column_name)
                       
                       # 2. Presidio Scan (Mock query)
                       import pandas as pd
                       try:
                           df = pd.read_sql(f"SELECT {col.column_name} FROM {table.table_name} LIMIT 10", connector.engine)
                           sample_vals = df[col.column_name].tolist()
                       except:
                           sample_vals = []
                       pres_res = presidio.scan_column(table.table_name, col.column_name, sample_vals)
                       
                       # 3. LLM Reasoning
                       policy = agent.classify_column(
                           table_name=table.table_name,
                           column_name_raw=col.column_name,
                           column_name_expanded=exp_name,
                           data_type=col.data_type,
                           statistical_profile=str(vars(col)),
                           top_values=str(col.top_values),
                           presidio_result=str(vars(pres_res)),
                           abbreviation_status=str(fully_res)
                       )
                       
                       # Queue low confidence for Human Review
                       if policy.llm_confidence < 0.6:
                           with self.db_client.get_session() as session:
                               queue_item = db_models.HumanReviewQueue(
                                   table_name=table.table_name,
                                   column_name=col.column_name,
                                   llm_best_guess=policy.model_dump(),
                                   flag_reason="Low Confidence Score"
                               )
                               session.add(queue_item)
                               session.commit()
                               
                       # Save Policy
                       with self.db_client.get_session() as session:
                            db_policy = session.query(db_models.ColumnPolicy).filter_by(table_name=table.table_name, column_name=col.column_name).first()
                            if not db_policy:
                                db_policy = db_models.ColumnPolicy(**policy.model_dump())
                                session.add(db_policy)
                            else:
                                for k, v in policy.model_dump().items():
                                    setattr(db_policy, k, v)
                            session.commit()
                       table_policies.append(policy)
                       
                  # 4. Generate Strategy
                  domain = domain_map.get(table.table_name, "unknown")
                  strategy = planner.generate_strategy(table.table_name, domain, table_policies)
                  with self.db_client.get_session() as session:
                      strat = db_models.GenerationStrategy(**strategy.model_dump())
                      session.merge(strat)
                      session.commit()

             # --- Phase 5: Synthesis (Stubbed for Orchestrator loop) ---
             self._update_status("Synthetic Generation", 80.0)
             # In complete loop, we'd wait for hrq clearance. 
             # For now, we simulate completion.
             
             self._update_status("Completed", 100.0, "completed")
             
        except Exception as e:
             logger.error(f"Pipeline execution failed: {e}")
             self._update_status(f"Failed: {str(e)[:100]}", 0.0, "failed")
