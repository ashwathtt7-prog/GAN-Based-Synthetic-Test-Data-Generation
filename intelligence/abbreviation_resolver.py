"""
Abbreviation Resolver
Expands telecom abbreviations using Neo4j dictionary and contextual heuristics.
"""

from graph.graph_tools import get_driver

class AbbreviationResolver:
    def __init__(self):
        pass

    def resolve_column_name(self, column_name: str) -> tuple[str, bool]:
        """
        Attempts to resolve a column name like CUST_TEN_MNT to Customer Tenure Month.
        Returns: (expanded_name, is_fully_resolved)
        """
        driver = get_driver()
        tokens = column_name.split('_')
        expanded_tokens = []
        fully_resolved = True
        
        with driver.session() as session:
            for token in tokens:
                # Query global abbreviation dictionary
                result = session.run(f"MATCH (a:AbbreviationDict {{id: 'global'}}) RETURN a.`{token}` as exp")
                record = result.single()
                
                if record and record["exp"]:
                    expanded_tokens.append(record["exp"])
                else:
                    # Token not in dictionary
                    expanded_tokens.append(token)
                    fully_resolved = False
                    
        return " ".join(expanded_tokens), fully_resolved

    def expand_value_pattern(self, top_values: list[dict], column_name: str) -> dict:
        """
        Analyzes value patterns. Even if abbreviation is unknown,
        knowing ACT=80%, SUS=15%, TRM=5% tells us it's a categorical status.
        """
        if not top_values:
            return {"pattern_type": "unknown"}
            
        # Example pattern inference
        num_unique_in_top = len(top_values)
        if num_unique_in_top <= 10 and "_CD" in column_name or "STAT" in column_name:
            return {"pattern_type": "finite_categorical"}
            
        # Check if values are mostly numeric
        mostly_numeric = all(str(v["value"]).replace(".","").isdigit() for v in top_values[:5])
        if mostly_numeric:
             return {"pattern_type": "numeric_measure"}
             
        return {"pattern_type": "high_cardinality_string"}
