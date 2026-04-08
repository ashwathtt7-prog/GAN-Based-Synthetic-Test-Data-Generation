"""
LangChain tools for the LLM to traverse the Neo4j Knowledge Graph.
"""

from langchain_core.tools import tool
from neo4j import GraphDatabase
import json
from config.config import load_config

# Singleton driver for tools
_driver = None

def get_driver():
    global _driver
    if _driver is None:
        config = load_config()
        nc = config['neo4j']
        _driver = GraphDatabase.driver(nc['uri'], auth=(nc['username'], nc['password']))
    return _driver

@tool
def get_table_schema(table_name: str) -> str:
    """Returns the schema and statistical profile of all columns in a table."""
    driver = get_driver()
    with driver.session() as session:
        result = session.run("""
        MATCH (t:Table {name: $table_name})-[:HAS_COLUMN]->(c:Column)
        RETURN t, collect(properties(c)) as columns
        """, table_name=table_name.upper())
        record = result.single()
        if not record:
            return f"Table {table_name} not found in knowledge graph."
        
        return json.dumps({
            "table": dict(record["t"]),
            "columns": record["columns"]
        }, indent=2)

@tool
def get_relationships(table_name: str) -> str:
    """Returns all tables that reference or are referenced by this table."""
    driver = get_driver()
    with driver.session() as session:
        result = session.run("""
        MATCH (t:Table {name: $table_name})-[r:RELATES_TO]-(other:Table)
        RETURN type(r) as rel_type, properties(r) as props, other.name as other_table
        """, table_name=table_name.upper())
        
        rels = [{"related_table": rec["other_table"], "details": rec["props"]} for rec in result]
        return json.dumps(rels, indent=2)

@tool
def get_downstream_tables(table_name: str) -> str:
    """Returns all tables downstream (child tables) up to 3 hops away."""
    driver = get_driver()
    with driver.session() as session:
        result = session.run("""
        MATCH path=(t:Table {name: $table_name})-[:RELATES_TO*1..3]->(downstream:Table)
        RETURN downstream.name as dt, length(path) as hops
        ORDER BY hops
        """, table_name=table_name.upper())
        
        downstream = []
        for rec in result:
             if rec["dt"] not in downstream:
                 downstream.append(rec["dt"])
        return json.dumps(downstream, indent=2)

@tool
def get_abbreviation(token: str) -> str:
    """Returns the expanded form of an abbreviation if it exists in the dictionary."""
    driver = get_driver()
    with driver.session() as session:
        # Dynamic property lookup
        result = session.run(f"""
        MATCH (a:AbbreviationDict {{id: 'global'}})
        RETURN a.`{token.upper()}` as expansion
        """)
        record = result.single()
        if record and record["expansion"]:
            return record["expansion"]
        return "null"

@tool
def get_domain(table_name: str) -> str:
    """Returns the business domain assigned to the table."""
    driver = get_driver()
    with driver.session() as session:
        result = session.run("""
        MATCH (t:Table {name: $table_name})
        RETURN t.domain as domain
        """, table_name=table_name.upper())
        record = result.single()
        if record and record["domain"]:
            return record["domain"]
        return "unknown"
