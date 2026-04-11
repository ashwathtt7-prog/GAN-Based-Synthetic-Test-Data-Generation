import yaml
from pathlib import Path


def load_config() -> dict:
    """Load configuration from config.yaml."""
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_data_sources(config: dict | None = None) -> list[dict]:
    """Return configured source databases."""
    resolved = config or load_config()
    return list(resolved.get("data_sources", []) or [])


def get_default_data_source(config: dict | None = None) -> dict:
    """Return the default source configuration."""
    sources = get_data_sources(config)
    if not sources:
        raise ValueError("No data sources are configured.")

    explicit_default = next((source for source in sources if source.get("default")), None)
    return explicit_default or sources[0]


def get_data_source(source_name: str | None = None, config: dict | None = None) -> dict:
    """Resolve a source configuration by name, or fall back to the configured default."""
    sources = get_data_sources(config)
    if not sources:
        raise ValueError("No data sources are configured.")

    if source_name:
        for source in sources:
            if source.get("name") == source_name:
                return source
        raise ValueError(f"Unknown data source '{source_name}'.")

    return get_default_data_source(config)
