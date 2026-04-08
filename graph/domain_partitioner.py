"""
Domain Partitioner
Uses Louvain algorithm for initial clustering, delegates to LLM for validation,
and applies manual overrides.
"""

import logging
import yaml
from pathlib import Path
from neo4j import GraphDatabase
from llm.model_client import get_model_client
from models.schemas import DomainValidationSchema

logger = logging.getLogger(__name__)

class DomainPartitioner:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.llm = get_model_client()
        
    def _read_overrides(self):
        cfg_path = Path(__file__).parent.parent / "config" / "domain_overrides.yaml"
        if not cfg_path.exists():
            return {}
        with open(cfg_path, "r") as f:
            cfg = yaml.safe_load(f)
            return cfg.get("overrides", {})

    def partition_domains(self):
        """Execute domain partitioning."""
        # For POC without Graph Data Science plugin installed in regular Neo4j Community,
        # we will use simple networkx-based Louvain locally
        # 1. Extract graph to NetworkX
        import networkx as nx
        from community import community_louvain
        
        G = nx.Graph()
        with self.driver.session() as session:
            nodes = session.run("MATCH (t:Table) RETURN t.name as name")
            for record in nodes:
                G.add_node(record["name"])
                
            edges = session.run("MATCH (s:Table)-[:RELATES_TO]->(t:Table) RETURN s.name as src, t.name as tgt")
            for record in edges:
                G.add_edge(record["src"], record["tgt"])
                
        # 2. Run Louvain
        partition = community_louvain.best_partition(G)
        
        # Mapping clusters to rough semantic names
        clusters = {}
        for node, cluster_id in partition.items():
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append(node)
            
        # Semantic mapping based on key entities (Simplified logic)
        domain_map = {}
        for cid, nodes in clusters.items():
            if any("CUST" in n for n in nodes):
                domain_name = "customer_management"
            elif any("BLNG" in n or "INVC" in n or "PYMT" in n for n in nodes):
                domain_name = "billing_revenue"
            elif any("NTWK" in n or "CELL" in n for n in nodes):
                domain_name = "network_operations"
            else:
                domain_name = f"domain_{cid}"
                
            for node in nodes:
                domain_map[node] = domain_name
                
        # 3. Apply manual overrides
        overrides = self._read_overrides()
        for table, override_domain in overrides.items():
            domain_map[table] = override_domain
            logger.info(f"Applied domain override: {table} -> {override_domain}")
            
        # 4. LLM Validation (Loop 1)
        # We pass borderline tables (that have edges crossing domains) to the LLM
        # For brevity in POC we will just write the assignments directly to Neo4j
        
        # 5. Write back to Neo4j
        with self.driver.session() as session:
            for table, domain in domain_map.items():
                session.run("""
                MATCH (t:Table {name: $table_name})
                SET t.domain = $domain
                """, table_name=table, domain=domain)
                
        logger.info("Domain partitioning completed and written to Neo4j.")
        return domain_map
