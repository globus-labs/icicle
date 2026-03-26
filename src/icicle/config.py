"""YAML configuration loader for icicle."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def load_config(path: str) -> dict[str, Any]:
    """Load a YAML configuration file.

    Args:
        path: Path to the YAML config file.

    Returns:
        Parsed config dict. Empty dict if file is empty.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the YAML content is not a mapping.
    """
    import yaml  # noqa: PLC0415

    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f'Config file not found: {path}')

    with config_path.open(encoding='utf-8') as f:
        data = yaml.safe_load(f)

    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(
            f'Config must be a YAML mapping, got {type(data).__name__}',
        )
    return data
