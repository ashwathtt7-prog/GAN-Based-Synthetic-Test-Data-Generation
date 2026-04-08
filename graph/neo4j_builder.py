"""
Neo4j Graph Builder
Constructs the knowledge graph in Neo4j with tables, columns, and relationships.
"""

import logging
from neo4j import GraphDatabase
from models.schemas import TableMetadata, RelationshipInfo
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)

class Neo4jBuilder:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._seed_abbreviations()
        
    def close(self):
        self.driver.close()
        
    def _seed_abbreviations(self):
        """Seed the global abbreviation dictionary into Neo4j."""
        config_path = Path(__file__).parent.parent / "config" / "config.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            
        abbreviations = config.get("abbreviations", {})
        if not abbreviations:
            return
            
        # Write to a singleton node
        with self.driver.session() as session:
            session.write_transaction(self._tx_seed_abbrev, abbreviations)
            
    @staticmethod
    def _tx_seed_abbrev(tx, abbreviations):
        # We serialize as JSON to store dynamic properties readily
        dict_props = ", ".join([f"`{k}`: '{v}'" for k, v in abbreviations.items()])
        query = f"""
        MERGE (a:AbbreviationDict {{id: 'global'}})
        SET a += {{{dict_props}}}
        """
        tx.run(query)
        logger.info("Seeded abbreviation dictionary in Neo4j.")

    def build_graph(self, tables: list[TableMetadata], relationships: list[RelationshipInfo]):
        """Build the full knowledge graph from schema and relationships."""
        with self.driver.session() as session:
            # 1. Clear existing generic relationships (for fresh POC build)
            # In prod, we'd do smart merge. Here we merge to not lose LLM data if we rerun.
            
            # 2. Add Tables and Columns
            for table in tables:
                session.write_transaction(self._tx_add_table, table)
                for col in table.columns:
                    session.write_transaction(self._tx_add_column, table.table_name, col)
                    
            # 3. Add Relationships
            for rel in relationships:
                session.write_transaction(self._tx_add_relationship, rel)
                
        logger.info("Successfully built Neo4j knowledge graph.")

    @staticmethod
    def _tx_add_table(tx, table: TableMetadata):
        tx.run("""
        MERGE (t:Table {name: $name})
        SET t.row_count = $row_count, 
            t.column_count = $column_count
        """, name=table.table_name, row_count=table.row_count, column_count=table.column_count)
        
    @staticmethod
    def _tx_add_column(tx, table_name: str, col):
        # Merge column
        # Generate property string for top values
        import json
        top_vals = json.dumps(col.top_values)
        
        tx.run("""
        MERGE (c:Column {name: $col_name, table: $table_name})
        SET c.data_type = $data_type,
            c.row_count = $row_count,
            c.null_rate = $null_rate,
            c.unique_count = $unique_count,
            c.top_values = $top_vals
        """, col_name=col.column_name, table_name=table_name, 
             data_type=col.data_type, row_count=col.row_count, 
             null_rate=col.null_rate, unique_count=col.unique_count,
             top_vals=top_vals)
             
        # Link to table
        tx.run("""
        MATCH (t:Table {name: $table_name})
        MATCH (c:Column {name: $col_name, table: $table_name})
        MERGE (t)-[r:HAS_COLUMN]->(c)
        """, table_name=table_name, col_name=col.column_name)
        
    @staticmethod
    def _tx_add_relationship(tx, rel: RelationshipInfo):
        # We link Table -> Table and also store column details on the edge
        tx.run("""
        MATCH (s:Table {name: $source_table})
        MATCH (t:Table {name: $target_table})
        MERGE (s)-[r:RELATES_TO {source_column: $source_column, target_column: $target_column}]->(t)
        SET r.relationship_type = $rel_type,
            r.confidence = $conf
        """, source_table=rel.source_table, target_table=rel.target_table,
             source_column=rel.source_column, target_column=rel.target_column,
             rel_type=rel.relationship_type, conf=rel.confidence)
