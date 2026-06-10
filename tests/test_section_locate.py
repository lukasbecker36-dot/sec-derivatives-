"""Tests for section_locate: scoring, locate-result application, heading learning."""

import yaml

from src.config import IssuerConfig, SectionConfig, FieldConfig
from src.section_locate import (
    score_section, assess_sections, build_locate_request, apply_locate_result,
    heading_to_regex, persist_learned_heading, MIN_SECTION_LENGTH,
)


def _section_cfg(**kwargs):
    defaults = dict(heading=r'Note\s+\d+\s*[.\-]?\s*Derivative',
                    validation_keywords=['notional', 'derivative'],
                    max_length=5000)
    defaults.update(kwargs)
    return SectionConfig(**defaults)


def _config(sections=None, fields=None):
    return IssuerConfig(
        issuer='Test Co', ticker='TST', cik='0000123456',
        sections=sections or {}, fields=fields or {},
    )


class TestScoreSection:
    def test_empty(self):
        assert score_section('', _section_cfg()) == 'empty'
        assert score_section('   \n ', _section_cfg()) == 'empty'

    def test_stub_cross_reference(self):
        text = 'For further details see Note 12 in the accompanying notes.'
        assert score_section(text, _section_cfg()) == 'stub'

    def test_stub_too_short(self):
        text = 'derivative notional ' * 5  # keywords present but tiny
        assert len(text) < MIN_SECTION_LENGTH
        assert score_section(text, _section_cfg()) == 'stub'

    def test_keyword_miss(self):
        text = 'x' * 100 + ' revenue grew this quarter across all segments ' + 'y' * 300
        assert score_section(text, _section_cfg()) == 'keyword_miss'

    def test_ok(self):
        text = ('The notional amounts of our derivative contracts were $5.2 billion. '
                * 10)
        assert score_section(text, _section_cfg()) == 'ok'


class TestAssessSections:
    def test_skips_sections_without_fields(self):
        sections = {
            'derivatives_note': _section_cfg(),
            'unused_section': _section_cfg(heading='Unused'),
        }
        fields = {'fx_notional': FieldConfig(description='FX notional',
                                             section='derivatives_note')}
        config = _config(sections, fields)
        out = assess_sections('no matching headings here', config)
        assert 'derivatives_note' in out
        assert 'unused_section' not in out
        assert out['derivatives_note']['status'] == 'empty'

    def test_ok_section_found(self):
        body = ('Note 5 - Derivative Instruments\n'
                + 'The notional amounts of our derivative contracts were $5.2 '
                  'billion as of period end. ' * 12
                + '\nNote 6 - Income Taxes')
        sections = {'derivatives_note': _section_cfg(
            heading=r'Note\s+\d+\s*[.\-]?\s*Derivative',
            end_boundary=r'Note\s+\d+\s*[.\-]?\s*(?!Derivative)')}
        fields = {'fx_notional': FieldConfig(description='d', section='derivatives_note')}
        out = assess_sections(body, _config(sections, fields))
        assert out['derivatives_note']['status'] == 'ok'
        assert 'notional' in out['derivatives_note']['text']


class TestBuildLocateRequest:
    def test_payload_shape(self):
        sections = {'derivatives_note': _section_cfg()}
        fields = {'fx_notional': FieldConfig(description='FX notional in millions',
                                             section='derivatives_note')}
        config = _config(sections, fields)
        text = 'blah ' * 100 + 'notional amounts of derivatives were $1 billion' + ' blah' * 100
        req = build_locate_request(text, config, 'derivatives_note',
                                   {'form_type': '10-Q', 'period_end': '2025-03-31',
                                    'accession_number': 'acc-1'},
                                   ['Note 5 - Derivative Instruments'])
        assert req['type'] == 'locate'
        assert req['section_name'] == 'derivatives_note'
        assert 'fx_notional' in req['prompt']
        assert 'Note 5 - Derivative Instruments' in req['prompt']
        assert '[WINDOW 1]' in req['prompt']


class TestApplyLocateResult:
    FILING = ('Intro text. ' * 50
              + 'Note 5 - Derivative Instruments. The notional amounts of our '
                'derivative contracts were $5.2 billion. '
              + 'More derivative hedge discussion with notional values. ' * 10
              + 'Note 6 - Income Taxes. Tax stuff follows.')

    def test_found_with_anchors(self):
        result = {'found': True,
                  'heading_text': 'Note 5 - Derivative Instruments',
                  'start_anchor': 'Note 5 - Derivative Instruments',
                  'end_anchor': 'Note 6 - Income Taxes'}
        text, status = apply_locate_result(self.FILING, result, _section_cfg())
        assert status == 'ok'
        assert text.startswith('Note 5 - Derivative Instruments')
        assert 'Income Taxes' not in text

    def test_whitespace_tolerant_anchor(self):
        result = {'found': True,
                  'start_anchor': 'Note 5  -  Derivative   Instruments',
                  'end_anchor': None}
        text, status = apply_locate_result(self.FILING, result, _section_cfg())
        assert status == 'ok'

    def test_not_disclosed(self):
        result = {'found': False, 'reason': 'not_disclosed',
                  'note': 'cross-references the 10-K'}
        text, status = apply_locate_result(self.FILING, result, _section_cfg())
        assert status == 'not_disclosed'
        assert text == ''

    def test_anchor_not_in_text(self):
        result = {'found': True, 'start_anchor': 'this string does not exist anywhere'}
        text, status = apply_locate_result(self.FILING, result, _section_cfg())
        assert status == 'locate_failed'

    def test_located_but_no_keywords_fails(self):
        filing = 'Random preamble. UNIQUE START HERE ' + 'unrelated text ' * 60
        result = {'found': True, 'start_anchor': 'UNIQUE START HERE'}
        text, status = apply_locate_result(filing, result, _section_cfg())
        assert status == 'locate_failed'


class TestHeadingToRegex:
    def test_note_heading_generalised(self):
        import re
        pattern = heading_to_regex('Note 12 — Derivative Financial Instruments')
        assert pattern.startswith(r'Note\s+\d+')
        # Must match renumbered variants
        assert re.search(pattern, 'Note 3. Derivative Financial Instruments')
        assert re.search(pattern, 'Note 12 – Derivative  Financial\nInstruments')

    def test_non_note_heading_escaped(self):
        import re
        pattern = heading_to_regex('Quantitative and Qualitative Disclosures')
        assert re.search(pattern, 'Quantitative and  Qualitative\nDisclosures')


class TestPersistLearnedHeading:
    def test_writes_heading_override(self, tmp_path):
        cfg_path = tmp_path / 'tst.yaml'
        cfg_path.write_text(yaml.safe_dump({
            'issuer': 'Test Co', 'ticker': 'TST', 'cik': '123',
            'archetype': 'minimal_hedger',
        }), encoding='utf-8')

        updated = persist_learned_heading(cfg_path, 'derivatives_note',
                                          r'Note\s+\d+\s*Derivatives')
        assert updated
        raw = yaml.safe_load(cfg_path.read_text(encoding='utf-8'))
        assert raw['sections']['derivatives_note']['heading'] == r'Note\s+\d+\s*Derivatives'
        # Other keys preserved
        assert raw['archetype'] == 'minimal_hedger'

    def test_noop_when_unchanged(self, tmp_path):
        cfg_path = tmp_path / 'tst.yaml'
        cfg_path.write_text(yaml.safe_dump({
            'issuer': 'T', 'ticker': 'T', 'cik': '1',
            'sections': {'derivatives_note': {'heading': 'X'}},
        }), encoding='utf-8')
        assert not persist_learned_heading(cfg_path, 'derivatives_note', 'X')

    def test_missing_file(self, tmp_path):
        assert not persist_learned_heading(tmp_path / 'nope.yaml', 's', 'X')
