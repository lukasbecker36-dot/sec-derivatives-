SEC DERIVATIVES PROJECT — NEXT STAGE IMPLEMENTATION BRIEF
=========================================================

You are working on an existing Python project that extracts derivatives and market risk data from SEC 10-Q / 10-K filings.

Current architecture
--------------------
The existing system already has:
- filing_fetcher.py: EDGAR discovery and filing download
- section_extract.py: deterministic section slicing
- llm_extract.py: Claude-based structured extraction
- engine.py: pipeline orchestration + CSV / notes / alerts writing
- validate.py: sanity checks
- change_detect.py: period-over-period alerts
- bootstrap.py: draft issuer config generation
- monitor.py: incremental monitoring for issuers that already have configs
- alerts.py: cross-issuer dashboard and story leads

The current config system uses:
- profiles/_archetypes/*.yaml
- profiles/{ticker}.yaml for live issuer configs

Current output system uses:
- output/{ticker}/tracking.csv
- output/{ticker}/notes.txt
- output/{ticker}/alert_log.txt

Important architectural principle:
- filings are NOT stored locally
- filings are fetched from EDGAR on demand
- only extracted outputs are persisted

This current design works for a small live-tested set of issuers, but does not scale well because bootstrap-generated configs still need manual review and rewrite. The next stage is to support a much larger universe, around 400 non-financial S&P 500 names, without manually onboarding each issuer up front.

Goal
----
Implement a git-backed universe registry plus lazy activation workflow.

Desired behaviour:
1. Existing configured issuers continue to run incrementally exactly as they do now.
2. A much larger universe of pre-registered issuers is tracked, but these issuers are NOT fully onboarded immediately.
3. When a pre-registered issuer files a new 10-Q or 10-K after system go-live, that filing triggers activation.
4. Activation should create a draft config just in time, run extraction on the fresh filing, and if successful make the issuer active.
5. After activation, the issuer joins the normal incremental monitoring flow.
6. Optionally perform a very small micro-backfill after first success (for example the most recent prior 10-K and/or 10-Q), but DO NOT do a full two-year historical onboarding for all 400 names up front.
7. This should be designed to run automatically in a git-based repo with scheduled execution, ideally via GitHub Actions.

High-level design to implement
------------------------------
Create a separate registry layer in addition to the existing profiles/ config layer.

New concepts:
- registered issuer: known in the universe, not yet fully configured
- activating issuer: new filing detected, activation in progress
- active issuer: has a working profile config and participates in normal monitor flow
- active_needs_review: activation succeeded but with weak confidence / warnings
- failed_activation: activation failed and needs review or retry

Design principle:
- profiles/ should contain only active or near-active issuer configs
- registry/ should contain the full universe list and activation metadata
- do not generate 400 stub YAML files in profiles/

Files / folders to add
----------------------
Please implement the following new repo structure:

registry/
  universe.csv
  activation_log.csv
  review_queue.csv

src/
  registry.py
  activation.py
  scheduler.py

Potentially also:
  storage.py
  notifier.py

Do not break the existing modules if avoidable. Extend them cleanly.

Registry file design
--------------------
Create registry/universe.csv as the canonical universe control file.

Suggested columns:
- ticker
- issuer_name
- cik
- sector
- status
- archetype_guess
- config_path
- first_seen_filing_date
- last_checked_at
- last_filing_date_seen
- last_processed_period
- last_activation_attempt_at
- activation_fail_count
- review_status
- notes

Minimum essential columns if you want to keep it lean:
- ticker
- issuer_name
- cik
- sector
- status
- archetype_guess
- config_path
- last_checked_at
- last_filing_date_seen
- last_processed_period

Status values:
- registered
- activating
- active
- active_needs_review
- failed_activation

Important behaviour:
- the 7 already live-tested issuers should be seeded as active
- the rest of the 400-name universe should be seeded as registered
- the registry is committed to git and acts as the lightweight source of truth for issuer lifecycle state

Activation flow
---------------
Implement lazy activation like this:

For a registered issuer:
1. Poll the SEC company submissions JSON using the issuer's CIK.
2. Detect if a new 10-Q or 10-K exists after the system's go-live cutoff or after the issuer's last known filing.
3. If no new filing exists:
   - update last_checked_at
   - leave status as registered
4. If a new filing exists:
   - mark status as activating
   - fetch the filing text
   - run bootstrap logic to generate a draft config from the filing
   - validate the generated config enough to see whether it is usable
   - run first extraction on the triggering filing
   - if extraction is strong enough, write profiles/{ticker}.yaml and create output/{ticker}/tracking.csv etc
   - then mark the issuer active (or active_needs_review)
   - optionally run a small micro-backfill on one or two recent prior filings
5. If activation fails badly:
   - mark failed_activation
   - append to review_queue.csv
   - do not let one bad activation crash the whole daily run

Activation scoring / promotion logic
------------------------------------
Implement a simple activation score or gate.

Example signals that should count positively:
- required section extraction is non-empty
- validation keywords are present
- key fields are extracted with non-low confidence
- market risk or derivatives note was found in a plausible-length section

Negative signals:
- empty or tiny extracted section
- likely cross-reference instead of real note
- too many null core fields
- validation errors
- suspicious units mismatch
- malformed config

Suggested promotion rules:
- strong result -> active
- usable but questionable -> active_needs_review
- poor result -> failed_activation

Review queue
------------
Create registry/review_queue.csv.

Suggested columns:
- timestamp
- ticker
- cik
- reason
- severity
- filing_date
- form_type
- config_path
- status

Populate this whenever:
- activation score is weak
- section extraction looks like a cross-reference
- a first-time config is probably wrong
- extraction has too many nulls
- validation fails materially

Activation log
--------------
Create registry/activation_log.csv to append lifecycle events such as:
- registered -> activating
- activating -> active
- activating -> active_needs_review
- activating -> failed_activation

Include timestamp, ticker, old_status, new_status, filing metadata, and reason.

Scheduler design
----------------
Implement src/scheduler.py as the main orchestrator for scheduled runs.

It should perform two passes:

PASS 1: ACTIVE ISSUERS
- load issuers with status active or active_needs_review
- run the existing incremental monitor flow for them
- process new filings exactly as the current monitor does

PASS 2: REGISTERED ISSUERS
- load issuers with status registered
- check EDGAR submissions JSON for newly filed 10-Q / 10-K
- trigger activation only for issuers where a new filing is detected

Requirements for scheduler:
- should produce a run summary
- should isolate issuer failures so one failure does not abort the full run
- should update registry files
- should be suitable for GitHub Actions / cron execution
- should return non-zero exit code only for true run-level failure, not for one issuer activation miss

Registry module requirements
----------------------------
Implement src/registry.py with clean helper functions such as:
- load_universe()
- save_universe(df)
- get_registered_issuers()
- get_active_issuers()
- update_last_checked(...)
- mark_activating(...)
- mark_active(...)
- mark_active_needs_review(...)
- mark_failed_activation(...)
- append_activation_event(...)
- append_review_item(...)

Please keep this simple, explicit, and CSV-based for now.

Monitor integration
-------------------
Refactor monitor.py if necessary so that it can:
- still run the current config-based incremental mode
- optionally accept a list of issuers passed in from scheduler.py rather than discovering issuers only from profiles/

Do not remove current CLI behaviour unless absolutely necessary.

Bootstrap integration
---------------------
Refactor bootstrap.py so it can be used in two ways:

1. Existing/manual usage:
- bootstrap a named issuer from CIK and write a draft config for manual review

2. New activation usage:
- generate a draft config in memory or at a specified output path
- return metadata about archetype guess, heading confidence, section stats, and any warnings
- support activation-mode usage without assuming a human is immediately reviewing the file

The current bootstrap quality is imperfect and tends to default too often, so implement a structure that exposes confidence and failure modes rather than pretending the draft is always production-ready.

Section extraction robustness
-----------------------------
The current project has known fragility around cross-reference filtering and 10-K vs 10-Q heading divergence.

Please improve for scale:
- add stronger cross-reference detection
- consider section length heuristics
- allow 10-K-specific heading overrides where appropriate
- avoid promoting an issuer to active if the extracted section is clearly just a cross-reference stub

Universe seeding
----------------
Implement a simple seeding path:
- seed existing live issuers into registry/universe.csv as active
- seed the rest of the supplied S&P non-financial universe as registered

Please assume that I already have the CIK list and that this can be loaded from a CSV or similar input file.

Git-backed workflow
-------------------
This project needs to work cleanly in a git repo.

Please design the implementation so that:
- registry/*.csv lives in git
- profiles/*.yaml lives in git
- code and workflow files live in git
- growing output files should ideally NOT be the main git persistence layer long-term

For now:
- it is acceptable if registry/config changes are committed back into the repo after a scheduled run
- but the design should keep open the option of storing output/ state somewhere better later

GitHub Actions target
---------------------
Please make the code easy to wire into a daily GitHub Actions workflow.

Assume the workflow will:
- checkout the repo
- install dependencies
- load secrets
- run python -m src.scheduler
- optionally commit updated registry/config files back to the repo
- send a summary notification

You do not need to build the full workflow first unless useful, but the code should be structured for this use case.

Notification / summary behaviour
--------------------------------
At minimum, produce a structured run summary including:
- how many active issuers checked
- how many registered issuers checked
- how many newly activated
- how many new filings processed
- which issuers had failures
- which issuers were sent to review_queue

A notifier module is optional, but a clean summary object or log output is important.

Implementation priorities
-------------------------
Implement in this order:

1. Add registry/ files and src/registry.py
2. Add issuer lifecycle states and seeding logic
3. Add src/activation.py
4. Add src/scheduler.py
5. Refactor bootstrap.py for activation-mode usage
6. Refactor monitor.py so scheduler can call it cleanly
7. Add review queue and activation log writing
8. Improve cross-reference and first-activation safeguards
9. Add tests for the new lifecycle logic

Testing expectations
--------------------
Add or update tests for:
- registry load/save and status transitions
- activation trigger when a new filing appears
- no activation when no new filing exists
- successful promotion registered -> active
- weak promotion registered -> active_needs_review
- failed activation -> failed_activation + review_queue
- scheduler two-pass orchestration
- resilience when one issuer fails but the rest continue

Coding style expectations
-------------------------
- keep the implementation simple and explicit
- prefer small pure functions where possible
- preserve current project style
- avoid overengineering
- avoid introducing a database at this stage
- CSV-based registry state is fine for now
- do not break existing issuer configs or current monitor usage

Deliverables
------------
Please produce:
1. the actual code changes
2. any new CSV templates or sample files needed
3. updated README / handover notes if useful
4. a brief note explaining how to seed the initial universe and run the scheduler

If useful, start by sketching the implementation plan and file diffs before writing code, but then proceed to make the changes.

Context / rationale
-------------------
The current project already works for a small set of configured issuers and tracks filings incrementally via tracking.csv. The problem is scaling: bootstrap currently generates stubs that need manual review, and onboarding hundreds of issuers one by one would take too long. The desired solution is to pre-register the full universe now, then activate each issuer only when it next files, so the coverage builds gradually over time while preserving the incremental behaviour for names already configured.