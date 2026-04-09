"""
Abbreviation Resolver
Expands telecom abbreviations using the in-memory knowledge graph dictionary.
"""

from graph.knowledge_graph import get_knowledge_graph


class AbbreviationResolver:
    def __init__(self):
        self.kg = get_knowledge_graph()

    def resolve_column_name(self, column_name: str) -> tuple[str, bool]:
        """
        Attempts to resolve a column name like CUST_TEN_MNT to Customer Tenure Month.
        Returns: (expanded_name, is_fully_resolved)
        """
        tokens = column_name.split('_')
        expanded_tokens = []
        fully_resolved = True

        for token in tokens:
            expansion = self.kg.get_abbreviation(token)
            if expansion != "null":
                expanded_tokens.append(expansion)
            else:
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

        num_unique_in_top = len(top_values)
        if num_unique_in_top <= 10 and ("_CD" in column_name or "STAT" in column_name):
            return {"pattern_type": "finite_categorical"}

        mostly_numeric = all(str(v.get("value", "")).replace(".", "").replace("-", "").isdigit()
                             for v in top_values[:5])
        if mostly_numeric:
            return {"pattern_type": "numeric_measure"}

        return {"pattern_type": "high_cardinality_string"}
