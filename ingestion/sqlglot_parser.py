"""
SQLGlot Parser
Parses DDL to extract declared foreign key relationships.
"""

import logging
from pathlib import Path
import sqlglot
from sqlglot import exp
from models.schemas import RelationshipInfo

logger = logging.getLogger(__name__)

class DDLParser:
    def __init__(self, ddl_dir: str):
        self.ddl_dir = Path(ddl_dir)
        
    def parse_relationships(self) -> list[RelationshipInfo]:
        """Extract foreign key relationships from all DDL files."""
        relationships = []
        
        for sql_file in self.ddl_dir.glob("*.sql"):
            with open(sql_file, "r") as f:
                content = f.read()
                
            try:
                # Parse the DDL
                parsed = sqlglot.parse(content)
                for statement in parsed:
                    if isinstance(statement, exp.Create):
                        # Find the table name
                        table_name = statement.this.name.upper()
                        
                        # Look for foreign key constraints in table schema
                        schema = statement.find(exp.Schema)
                        if schema:
                            for expr in schema.expressions:
                                # Foreign keys defined inline or as table constraints
                                # In sqlglot representation, look for Reference or ForeignKey
                                if isinstance(expr, exp.ForeignKey):
                                    source_col = [col.name.upper() for col in expr.expressions][0]
                                    target_table = expr.args.get('reference').this.name.upper()
                                    target_col = [col.name.upper() for col in expr.args.get('reference').expressions][0]
                                    
                                    rel = RelationshipInfo(
                                        source_table=table_name,
                                        source_column=source_col,
                                        target_table=target_table,
                                        target_column=target_col,
                                        relationship_type="FK_DECLARED",
                                        confidence=1.0 # High confidence as it's explicitly declared
                                    )
                                    relationships.append(rel)
                                    
            except Exception as e:
                logger.error(f"Error parsing DDL file {sql_file.name}: {e}")
                
        return relationships
