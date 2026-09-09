"""Every archetype ships a `Newsroom signals` qualitative category, and
its patterns fire on the sentence shapes a Risk.net reporter would
actually lead a story with. Without this, expanding editorial coverage
means editing four YAMLs and hoping no regex was fat-fingered.

The digest routine's Step 3 leads the cross-cutting section with
whatever appears under this category name, so silent drift here shows
up as a suddenly-empty section in the daily email — not a test
failure. These tests catch it in CI instead.
"""

from pathlib import Path

import pytest
import yaml

from src.qualitative import extract_qualitative
from src.config import IssuerConfig, QualitativeConfig, SectionConfig

ARCHETYPES_DIR = Path(__file__).resolve().parent.parent / 'profiles' / '_archetypes'
ARCHETYPE_FILES = sorted(ARCHETYPES_DIR.glob('*.yaml'))


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding='utf-8'))


class TestNewsroomSignalsPresent:
    @pytest.mark.parametrize('archetype', ARCHETYPE_FILES, ids=lambda p: p.stem)
    def test_category_exists(self, archetype):
        cfg = _load(archetype)
        cats = cfg.get('qualitative', {}).get('categories', {})
        assert 'Newsroom signals' in cats, (
            f"{archetype.name} is missing the 'Newsroom signals' category; "
            "the daily digest's cross-cutting section will fall through."
        )
        patterns = cats['Newsroom signals']
        assert isinstance(patterns, list) and patterns, (
            f"{archetype.name} Newsroom signals must be a non-empty list"
        )


def _make_config(patterns):
    return IssuerConfig(
        issuer='Test', ticker='TEST', cik='0000000001',
        qualitative=QualitativeConfig(
            sections_to_search=['market_risk'],
            categories={'Newsroom signals': patterns},
        ),
        sections={'market_risk': SectionConfig(heading='test')},
    )


# Representative sentence shapes a reporter would actually lead a story
# with. The full active-hedger keyword pool must hit every one; if any
# fall through, the pool no longer represents the editorial brief.
LEAD_SENTENCES = [
    # New programme initiation
    'The Company commenced hedging its net investment in European '
    'operations during the second quarter.',
    'We initiated a new cash flow hedging program covering forecasted '
    'aluminum purchases.',
    'For the first time, the Company designated a portion of its yen-'
    'denominated notes as a net investment hedge.',
    # Programme expansion
    'During the quarter we entered into additional interest rate swaps '
    'with a combined notional of $2.5 billion.',
    'The Company increased notional coverage of forecasted euro '
    'receipts by $800 million.',
    # Termination / unwind
    'The Company terminated certain interest rate swaps early, '
    'realising a gain of $45 million.',
    'We unwound a series of cross-currency swaps executed in 2023.',
    'The Company closed out its outstanding fuel collars in July.',
    # M&A / deal-contingent
    'In connection with the pending acquisition, the Company entered '
    'into a deal-contingent forward contract.',
    'The acquisition-related hedges were designated as cash flow '
    'hedges of the forecasted purchase price.',
    # Novation / documentation
    'Certain swaps were novated to a central counterparty during the '
    'first quarter.',
    'The Company amended its ISDA master agreement with two dealer '
    'counterparties to add zero-threshold CSAs.',
]


class TestNewsroomSignalsFireOnLeadSentences:
    """The pool has to hit every representative shape. If a sentence
    falls through, the routine won't surface it — which is the whole
    failure mode this work exists to prevent."""

    @pytest.mark.parametrize('archetype', ARCHETYPE_FILES, ids=lambda p: p.stem)
    def test_active_archetypes_catch_all(self, archetype):
        cfg = _load(archetype)
        patterns = cfg['qualitative']['categories']['Newsroom signals']
        # The no_derivatives pool is deliberately narrower — those
        # filers have no existing programme to expand or terminate,
        # so only first-use / M&A sentences must fire.
        if archetype.stem == 'no_derivatives':
            required = [
                s for s in LEAD_SENTENCES
                if 'commenced hedging' in s
                or 'initiated a new' in s
                or 'For the first time' in s
                or 'deal-contingent' in s
                or 'acquisition-related hedges' in s
            ]
        else:
            required = LEAD_SENTENCES
        config = _make_config(patterns)
        for sent in required:
            result = extract_qualitative({'market_risk': sent}, config)
            assert result.get('Newsroom signals'), (
                f"{archetype.name} Newsroom signals failed to match:\n"
                f"  {sent!r}"
            )


class TestNewsroomSignalsAreNotOverfired:
    """Guard against a pattern so loose it fires on routine language.
    If every filing produces a Newsroom signals hit, the category
    stops being a signal and becomes noise."""

    NOISE_SENTENCES = [
        'The fair value of derivative instruments is presented in the '
        'table below.',
        'Cash and cash equivalents are recorded at fair value.',
        'The interest rate on our senior notes is fixed at 4.25%.',
        'We are exposed to foreign currency exchange rate risk.',
    ]

    @pytest.mark.parametrize('archetype', ARCHETYPE_FILES, ids=lambda p: p.stem)
    def test_no_false_positives(self, archetype):
        cfg = _load(archetype)
        patterns = cfg['qualitative']['categories']['Newsroom signals']
        config = _make_config(patterns)
        for sent in self.NOISE_SENTENCES:
            result = extract_qualitative({'market_risk': sent}, config)
            assert not result.get('Newsroom signals'), (
                f"{archetype.name} Newsroom signals over-fired on "
                f"routine sentence: {sent!r}"
            )
