"""
Query Log Miner
Parses query logs to discover implicit JOIN patterns not declared as FKs.
"""

import logging
from pathlib import Path
import sqlglot
from sqlglot import exp
from models.schemas import RelationshipInfo
from collections import defaultdict

logger = logging.getLogger(__name__)

class QueryLogMiner:
    def __init__(self, log_dir: str):
        self.log_dir = Path(log_dir)
        
    def _extract_joins(self, ast) -> list[tuple]:
        """Traverse AST to find explicit and implicit JOINs."""
        joins = []
        
        # We look for queries that join tables
        for select in ast.find_all(exp.Select):
            # Tables used in query
            tables = {t.alias_or_name.upper(): t.name.upper() for t in select.find_all(exp.Table)}
            
            # Find JOIN ON conditions
            for join in select.find_all(exp.Join):
                if join.args.get("on"):
                    on_expr = join.args["on"]
                    if isinstance(on_expr, exp.EQ):
                        left = on_expr.left
                        right = on_expr.right
                        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                            t1_alias = left.table.upper() if left.table else None
                            t2_alias = right.table.upper() if right.table else None
                            
                            if t1_alias in tables and t2_alias in tables:
                                joins.append((
                                    tables[t1_alias], left.name.upper(),
                                    tables[t2_alias], right.name.upper()
                                ))
                                
            # Inferred joins from WHERE clauses (t1.id = t2.id)
            if select.args.get("where"):
                where = select.args["where"].this
                # We do a simplified check for WHERE t1.c1 = t2.c2
                for eq in where.find_all(exp.EQ):
                    left = eq.left
                    right = eq.right
                    if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                        t1_alias = left.table.upper() if left.table else None
                        t2_alias = right.table.upper() if right.table else None
                        
                        if t1_alias in tables and t2_alias in tables and t1_alias != t2_alias:
                            joins.append((
                                tables[t1_alias], left.name.upper(),
                                tables[t2_alias], right.name.upper()
                            ))
                            
        return joins

    def mine_relationships(self) -> list[RelationshipInfo]:
        """Mine relationships from query log frequencies."""
        relationship_counts = defaultdict(int)
        
        if not self.log_dir.exists():
            return []
            
        for sql_file in self.log_dir.glob("*.sql"):
            with open(sql_file, "r") as f:
                queries = f.read().split(";")
                
            for query in queries:
                if not query.strip():
                    continue
                try:
                    parsed = sqlglot.parse_one(query)
                    joins = self._extract_joins(parsed)
                    for join in joins:
                        # Serialize join pair consistently to count them
                        t1, c1, t2, c2 = join
                        # Alphabetical ordering to ensure A->B and B->A count as same relationship for mining
                        if t1 > t2:
                            pair = (t2, c2, t1, c1)
                        else:
                            pair = (t1, c1, t2, c2)
                        relationship_counts[pair] += 1
                except Exception as e:
                    logger.debug(f"Could not parse query for mining: {e}")
                    
        relationships = []
        for (t1, c1, t2, c2), count in relationship_counts.items():
            # Confidence based on count
            if count > 1000:
                conf = 0.95
            elif count > 100:
                conf = 0.80
            elif count > 10:
                conf = 0.60
            else:
                conf = 0.30
                
            relationships.append(RelationshipInfo(
                source_table=t1,
                source_column=c1,
                target_table=t2,
                target_column=c2,
                relationship_type="FK_INFERRED",
                confidence=conf
            ))
            
        logger.info(f"Mined {len(relationships)} inferred relationships from query logs.")
        return relationships
