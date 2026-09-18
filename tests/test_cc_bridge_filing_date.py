"""Regression: cc_bridge.prepare must propagate `filing_date` through the
extraction request payload so cc_bridge.finalize can write it back to
tracking.csv.

The bug this pins: filing_fetcher populates filing_date in filing_meta,
but prepare() previously dropped it when constructing the request JSON.
finalize() then read `req_data.get('filing_date', '')` and stored an
empty string, which cascaded into the daily digest manifest — every new
row's filing_date was blank, so the manifest classified every extraction
as a backfill (per "a genuinely-new filing always carries a date") and
State C "quiet day, N backfilled" emails hid the real news for a week.
"""
import inspect
from src import cc_bridge


class TestFilingDatePropagation:
    def test_extraction_request_carries_filing_date(self):
        """The extraction request built during prepare() must include
        filing_date. Without it, tracking.csv gets a blank filing_date
        and the digest manifest sees every row as a backfill."""
        src = inspect.getsource(cc_bridge.prepare)
        # The request_data dict for extraction must reference filing_meta's
        # filing_date. If a refactor moves the dict but drops the key,
        # this test catches it.
        assert "'filing_date': filing_meta.get('filing_date'" in src

    def test_bootstrap_request_carries_filing_date(self):
        """The bootstrap (activation) request must also carry filing_date
        so first-time extractions land with a populated column."""
        src = inspect.getsource(cc_bridge.prepare)
        assert "'filing_date': new_filing.get('filing_date'" in src

    def test_finalize_reads_filing_date_from_request(self):
        """The finalize side reads req_data['filing_date'] onto the row.
        Preserves the contract that a request key of that name must
        exist for the write to have anything to copy."""
        src = inspect.getsource(cc_bridge.finalize)
        assert "row['filing_date'] = req_data.get('filing_date'" in src
