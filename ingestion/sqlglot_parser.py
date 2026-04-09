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
                        schema = statement.this if isinstance(statement.this, exp.Schema) else statement.find(exp.Schema)
                        if not isinstance(schema, exp.Schema) or not isinstance(schema.this, exp.Table):
                            continue

                        table_name = schema.this.name.upper()
                        
                        # Look for foreign key constraints in table schema
                        for expr in schema.expressions:
                            if not isinstance(expr, exp.ForeignKey):
                                continue

                            reference = expr.args.get("reference")
                            target_schema = reference.this if reference else None
                            if not isinstance(target_schema, exp.Schema) or not isinstance(target_schema.this, exp.Table):
                                continue

                            source_cols = [col.name.upper() for col in expr.expressions if hasattr(col, "name")]
                            target_cols = [col.name.upper() for col in target_schema.expressions if hasattr(col, "name")]

                            for source_col, target_col in zip(source_cols, target_cols):
                                rel = RelationshipInfo(
                                    source_table=table_name,
                                    source_column=source_col,
                                    target_table=target_schema.this.name.upper(),
                                    target_column=target_col,
                                    relationship_type="FK_DECLARED",
                                    confidence=1.0
                                )
                                relationships.append(rel)
                                    
            except Exception as e:
                logger.error(f"Error parsing DDL file {sql_file.name}: {e}")
                
        return relationships
