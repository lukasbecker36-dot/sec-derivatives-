"""YAML config loader with archetype inheritance via deep merge."""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


PROFILES_DIR = Path(__file__).resolve().parent.parent / 'profiles'
ARCHETYPES_DIR = PROFILES_DIR / '_archetypes'


@dataclass
class SectionConfig:
    heading: str
    match_strategy: str = 'last'  # 'first' or 'last'
    validation_keywords: list[str] = field(default_factory=list)
    end_boundary: str = ''
    max_length: int = 10000


@dataclass
class FieldConfig:
    description: str
    section: str
    alert_threshold: Optional[float] = None


@dataclass
class QualitativeConfig:
    sections_to_search: list[str] = field(default_factory=list)
    categories: dict[str, list[str]] = field(default_factory=dict)


@dataclass
class IssuerConfig:
    issuer: str
    ticker: str
    cik: str
    archetype: str = ''
    sector: str = ''
    extraction_mode: str = 'llm'
    sections: dict[str, SectionConfig] = field(default_factory=dict)
    fields: dict[str, FieldConfig] = field(default_factory=dict)
    qualitative: QualitativeConfig = field(default_factory=QualitativeConfig)
    alert_thresholds: dict[str, float] = field(default_factory=dict)
    dashboard_mapping: dict[str, str] = field(default_factory=dict)


def _deep_merge(base: dict, override: dict) -> dict:
    """Deep merge override into base. Override wins on conflicts."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _load_yaml(path: Path) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


def load_config(yaml_path: Path) -> IssuerConfig:
    """Load issuer config YAML, resolving archetype inheritance."""
    raw = _load_yaml(yaml_path)

    # Resolve archetype
    archetype_name = raw.get('archetype', '')
    if archetype_name:
        arch_path = ARCHETYPES_DIR / f'{archetype_name}.yaml'
        if arch_path.exists():
            arch_data = _load_yaml(arch_path)
            raw = _deep_merge(arch_data, raw)

    # Parse sections
    sections = {}
    for name, sec_data in raw.get('sections', {}).items():
        sections[name] = SectionConfig(
            heading=sec_data.get('heading', ''),
            match_strategy=sec_data.get('match_strategy', 'last'),
            validation_keywords=sec_data.get('validation_keywords', []),
            end_boundary=sec_data.get('end_boundary', ''),
            max_length=sec_data.get('max_length', 10000),
        )

    # Parse fields
    fields = {}
    for name, fld_data in raw.get('fields', {}).items():
        fields[name] = FieldConfig(
            description=fld_data.get('description', ''),
            section=fld_data.get('section', ''),
            alert_threshold=fld_data.get('alert_threshold'),
        )

    # Parse qualitative
    qual_raw = raw.get('qualitative', {})
    qualitative = QualitativeConfig(
        sections_to_search=qual_raw.get('sections_to_search', []),
        categories=qual_raw.get('categories', {}),
    )

    # Apply per-field alert thresholds from alert_thresholds section
    alert_thresholds = raw.get('alert_thresholds', {})
    for fname, threshold in alert_thresholds.items():
        if fname in fields:
            fields[fname].alert_threshold = threshold

    return IssuerConfig(
        issuer=raw.get('issuer', ''),
        ticker=raw.get('ticker', ''),
        cik=raw.get('cik', ''),
        archetype=raw.get('archetype', ''),
        sector=raw.get('sector', ''),
        extraction_mode=raw.get('extraction_mode', 'llm'),
        sections=sections,
        fields=fields,
        qualitative=qualitative,
        alert_thresholds=alert_thresholds,
        dashboard_mapping=raw.get('dashboard_mapping', {}),
    )


def list_issuers(profiles_dir: Path = PROFILES_DIR) -> list[Path]:
    """Return all issuer YAML config paths (excluding archetypes)."""
    return sorted([
        p for p in profiles_dir.glob('*.yaml')
        if not p.name.startswith('_')
    ])
