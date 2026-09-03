"""Test the max_filings cap in cc_bridge.prepare.

The Sep 3 2026 scheduler fire tried to drain 186+ blank rows unlocked by the
PR #14 fix in a single run and blew the session token budget. The cap makes
runs bounded regardless of backlog size — the leftover drains over
subsequent daily fires.
"""

from unittest.mock import patch, MagicMock

import pytest

from src.cc_bridge import prepare, DEFAULT_MAX_FILINGS_PER_RUN


def _fake_universe():
    """A universe of 5 active issuers, each with a valid config_path stub."""
    return [
        {'ticker': f'T{i}', 'cik': str(1000 + i), 'status': 'active',
         'config_path': f'profiles/t{i}.yaml'}
        for i in range(5)
    ]


def _fake_unprocessed_20():
    """Twenty filings pending for one issuer — enough to hit the cap alone."""
    return [
        {'period_end': f'2025-{m:02d}-01', 'form_type': '10-Q',
         'accession_number': f'a{m}', 'primary_document': 'doc.htm'}
        for m in range(1, 13)
    ] + [
        {'period_end': f'2026-{m:02d}-01', 'form_type': '10-Q',
         'accession_number': f'b{m}', 'primary_document': 'doc.htm'}
        for m in range(1, 9)
    ]


class TestMaxFilingsCap:
    @patch('src.cc_bridge.load_universe')
    @patch('src.cc_bridge.get_active')
    @patch('src.cc_bridge.get_registered', return_value=[])
    @patch('src.cc_bridge.get_failed', return_value=[])
    @patch('src.cc_bridge._resolve_config_path')
    @patch('src.cc_bridge.load_config')
    @patch('src.cc_bridge.get_unprocessed_filings')
    @patch('src.cc_bridge.fetch_filing_text', return_value='Filing text.')
    @patch('src.cc_bridge.extract_all_sections',
           return_value={'derivatives_note': ''})
    def test_cap_stops_processing(self, mock_sections, mock_fetch,
                                   mock_unprocessed, mock_load_config,
                                   mock_resolve, mock_failed, mock_registered,
                                   mock_active, mock_load_universe, tmp_path):
        mock_load_universe.return_value = _fake_universe()
        mock_active.return_value = _fake_universe()
        mock_resolve.return_value = tmp_path / 'stub.yaml'
        (tmp_path / 'stub.yaml').write_text('stub')
        cfg = MagicMock()
        cfg.ticker = 'T0'; cfg.issuer = 'Test'; cfg.cik = '1000'; cfg.sector = ''
        cfg.fields = {}
        mock_load_config.return_value = cfg
        # Every active issuer has 20 pending filings — total backlog 100.
        mock_unprocessed.return_value = _fake_unprocessed_20()

        with patch('src.cc_bridge._write_manifest'), \
             patch('src.cc_bridge.WORK_DIR', tmp_path / 'work'), \
             patch('src.cc_bridge.get_or_create_profile', return_value={}), \
             patch('src.cc_bridge.build_prompt_context', return_value=''):
            prepare(since='2025-01-01', max_activations=10, max_filings=15)

        # fetch_filing_text is called once per queued filing. With a cap of
        # 15 the pass MUST stop at 15, not process the whole 100-item backlog.
        assert mock_fetch.call_count == 15

    @patch('src.cc_bridge.load_universe')
    @patch('src.cc_bridge.get_active')
    @patch('src.cc_bridge.get_registered', return_value=[])
    @patch('src.cc_bridge.get_failed', return_value=[])
    @patch('src.cc_bridge._resolve_config_path')
    @patch('src.cc_bridge.load_config')
    @patch('src.cc_bridge.get_unprocessed_filings')
    @patch('src.cc_bridge.fetch_filing_text', return_value='Filing text.')
    @patch('src.cc_bridge.extract_all_sections',
           return_value={'derivatives_note': ''})
    def test_cap_zero_disables(self, mock_sections, mock_fetch,
                                mock_unprocessed, mock_load_config,
                                mock_resolve, mock_failed, mock_registered,
                                mock_active, mock_load_universe, tmp_path):
        """max_filings=0 restores legacy no-cap behaviour for manual runs."""
        mock_load_universe.return_value = _fake_universe()
        mock_active.return_value = _fake_universe()
        mock_resolve.return_value = tmp_path / 'stub.yaml'
        (tmp_path / 'stub.yaml').write_text('stub')
        cfg = MagicMock()
        cfg.ticker = 'T0'; cfg.issuer = 'Test'; cfg.cik = '1000'; cfg.sector = ''
        cfg.fields = {}
        mock_load_config.return_value = cfg
        mock_unprocessed.return_value = _fake_unprocessed_20()

        with patch('src.cc_bridge._write_manifest'), \
             patch('src.cc_bridge.WORK_DIR', tmp_path / 'work'), \
             patch('src.cc_bridge.get_or_create_profile', return_value={}), \
             patch('src.cc_bridge.build_prompt_context', return_value=''):
            prepare(since='2025-01-01', max_activations=10, max_filings=0)

        # 5 issuers × 20 filings = 100 — legacy behaviour drains everything.
        assert mock_fetch.call_count == 100

    def test_default_is_thirty(self):
        assert DEFAULT_MAX_FILINGS_PER_RUN == 30
