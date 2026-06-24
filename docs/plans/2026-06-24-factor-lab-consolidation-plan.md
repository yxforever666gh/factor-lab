# Factor Lab Consolidation Plan

> **For Hermes:** This is a consolidation/checkpoint plan, not a feature expansion plan. Do not use this plan to add new research capability, run broad daemon workflows, generate strategy prototypes, write to queues, or enable live trading. Use ordinary repository inspection, tests, and minimal cleanup. If later implementation is needed, execute task-by-task and stop at each gate for verification.

**Goal:** Turn the current scattered Factor Lab workspace into a clean, auditable, test-backed checkpoint that separates engineering progress from unresolved research and approval gates.

**Architecture:** Preserve the two active workstreams as separate review units: Small Institutionalization / Paper Portfolio and Market Phenomenon Research Agent. Each workstream must have one canonical status source, explicit blocked/approved states, and a narrow test command. Runtime artifacts should be retained only when they are canonical evidence or useful examples; transient run output should be archived or ignored.

**Tech Stack:** Python 3, pytest, JSON/Markdown artifacts, existing Factor Lab package under `src/factor_lab`, scripts under `scripts/`, plans under `docs/plans/`, knowledge under `knowledge/`.

---

## Current Snapshot

Observed on 2026-06-24 from `/home/admin/factor-lab`.

### Git state

- Branch: `main`
- Modified tracked files: 7
- Untracked files: 70+
- Main untracked groups:
  - `src/`: 26
  - `tests/`: 26
  - `scripts/`: 24
  - `knowledge/`: 4
  - `docs/`: 2

### Verification already run

```bash
pytest tests/test_small_institutionalization_policy.py \
       tests/test_simulated_portfolio_construction_repair.py \
       tests/test_market_phenomena_*.py \
       tests/test_paper_portfolio_weekly_report.py \
       tests/test_small_institutional_operator_pending_consistency_snapshot.py \
       tests/test_small_institutional_operator_pending_observation.py -q
```

Observed result:

```text
238 passed in 5.60s
```

### Current runtime safety state

From `artifacts/research_daemon_status.json`:

- Daemon state: `idle`
- Mode: `interactive`
- Route status: healthy
- Exit reason: `no_admitted_workflow`
- Broad daemon execution remains inappropriate during consolidation.

---

## Non-Goals

Do **not** do any of the following during this consolidation:

- Do not create new alpha research capability.
- Do not run broad daemon workflows.
- Do not write to the research queue.
- Do not generate strategy prototypes unless a later human approval explicitly authorizes it.
- Do not enable production execution or live trading.
- Do not treat runtime success as proof of factor quality.
- Do not submit all artifacts blindly.
- Do not delete files permanently without a reviewed cleanup list; use archive/trash style cleanup.

---

## Workstream A: Small Institutionalization / Paper Portfolio

### Files in scope

Tracked/modified:

- `knowledge/small_institutionalization.md`
- `scripts/write_simulated_portfolio_construction_repair.py`
- `scripts/write_small_institutionalization_status.py`
- `src/factor_lab/simulated_portfolio_construction_repair.py`
- `src/factor_lab/small_institutionalization_policy.py`
- `tests/test_simulated_portfolio_construction_repair.py`
- `tests/test_small_institutionalization_policy.py`

Untracked/new candidates:

- `scripts/write_paper_portfolio_weekly_report.py`
- `scripts/write_small_institutional_operator_pending_consistency_snapshot.py`
- `scripts/write_small_institutional_operator_pending_observation.py`
- `src/factor_lab/paper_portfolio_weekly_report.py`
- `src/factor_lab/small_institutional_operator_pending_consistency_snapshot.py`
- `src/factor_lab/small_institutional_operator_pending_observation.py`
- `tests/test_paper_portfolio_weekly_report.py`
- `tests/test_small_institutional_operator_pending_consistency_snapshot.py`
- `tests/test_small_institutional_operator_pending_observation.py`

### Canonical status source

Use:

```text
knowledge/small_institutionalization.md
```

This file should remain the canonical human-readable state for the workstream.

### Required state to preserve

Current status must remain explicit:

- Phase: `A_baseline`
- Decision: `ready_for_portfolio_mvp`
- Next action: `repair_simulated_portfolio_construction`
- Paper portfolio ready: true
- Benchmark: `CSI1000`
- Position count: 72
- Primary blocker: `drawdown_risk_too_high`
- Simulated repair status: `blocked_no_drawdown_safe_candidate`
- Best available max drawdown: `-0.478256`
- Drawdown gap to limit: `0.128256`
- Required decision axis: `holding_count=50`
- Human approval present: false
- Queue write allowed: false
- Broad daemon allowed: false
- Automation allowed: false
- Automated rerun allowed: false
- Live trading enabled: false

### Acceptance criteria

- Status artifacts agree that the workstream is awaiting operator decision.
- Drawdown risk is not hidden or reclassified as solved.
- All automation/live/queue gates remain closed.
- Relevant tests pass.

### Verification command

```bash
pytest tests/test_small_institutionalization_policy.py \
       tests/test_simulated_portfolio_construction_repair.py \
       tests/test_paper_portfolio_weekly_report.py \
       tests/test_small_institutional_operator_pending_consistency_snapshot.py \
       tests/test_small_institutional_operator_pending_observation.py -q
```

---

## Workstream B: Market Phenomenon Research Agent

### Files in scope

Plan/docs:

- `docs/plans/2026-06-04-market-phenomenon-research-agent-plan.md`
- `docs/plans/2026-06-04-phenomenon-first-autonomous-research-agent-plan.md` — review as possible duplicate/obsolete draft.

Knowledge:

- `knowledge/market_phenomena_data_requests.json`
- `knowledge/market_phenomena_lessons.md`
- `knowledge/market_phenomena_memory.json`

Source:

- `src/factor_lab/market_phenomena_*.py`

Scripts:

- `scripts/market_phenomena_*.py`

Tests:

- `tests/test_market_phenomena_*.py`

Artifacts for evidence/review:

- `artifacts/market_phenomena/research_loop.md`
- `artifacts/market_phenomena/controlled_research_verdict.md`
- `artifacts/market_phenomena/deeper_oos_horizon_report.md`
- `artifacts/market_phenomena/human_review_pack.md`
- `artifacts/market_phenomena/strategy_design_approval_gate.md`

### Canonical status sources

Use the following canonical status chain:

1. `knowledge/market_phenomena_lessons.md` — accumulated lessons.
2. `knowledge/market_phenomena_memory.json` — structured memory.
3. `artifacts/market_phenomena/human_review_pack.md` — current human review summary.
4. `artifacts/market_phenomena/strategy_design_approval_gate.md` — current approval gate.

### Required state to preserve

Current research status must remain explicit:

- Research loop completed artifact-only.
- `quality_repair_delayed_repricing_v1` rejected after failed minimal verification.
- `value_trap_escape_after_balance_sheet_repair_v1` supported for further research.
- Only 20d horizon is currently supported for review.
- 5d rejected due to OOS negative spread.
- 60d rejected due to risk gate failure.
- 120d rejected due to OOS negative spread.
- Strategy generation allowed: false.
- Prototype generation allowed: false until human approval.
- Production execution allowed: false.
- Queue write allowed: false.
- Live trading allowed: false.

### Plan canonicalization decision

Default recommendation:

- Keep `docs/plans/2026-06-04-market-phenomenon-research-agent-plan.md` as canonical.
- Review `docs/plans/2026-06-04-phenomenon-first-autonomous-research-agent-plan.md` as likely obsolete/duplicate. Archive or remove only after confirming it does not contain unique accepted requirements.

### Acceptance criteria

- New namespace remains `market_phenomena_*`.
- The workstream is framed as phenomenon/mechanism research, not automatic strategy generation.
- Human approval gate blocks prototype generation.
- Tests pass.

### Verification command

```bash
pytest tests/test_market_phenomena_*.py -q
```

---

## Workstream C: Artifacts, Docs, and Cleanup

### Artifact policy

Classify artifacts into three groups:

1. **Canonical evidence** — keep and optionally commit if small and intentionally versioned.
2. **Example/schema artifacts** — keep only when tests/docs need examples.
3. **Transient runtime output** — archive, ignore, or leave uncommitted.

### Keep candidates

- Human review packs.
- Approval gate summaries.
- Minimal compact status reports.
- Structured knowledge files used as current state.

### Cleanup candidates

- Duplicate plans.
- Intermediate run artifacts superseded by review packs.
- One-off execution outputs that are not read by code/tests/docs.
- Script wrappers that duplicate another maintained script without adding contract value.

### Safety rule

Do not permanently delete during first pass. Move questionable files to an archive path or use recoverable trash after producing a reviewed cleanup list.

---

## Task Plan

### Task 1: Produce workspace inventory

**Objective:** Create a reviewed classification of all modified/untracked files.

**Files:**

- Create or update: `artifacts/consolidation/file_inventory_2026-06-24.md`

**Steps:**

1. Run `git status --short`.
2. Classify each file as one of:
   - `small_institutionalization`
   - `market_phenomena`
   - `docs_plan`
   - `knowledge`
   - `artifact_runtime`
   - `cleanup_candidate`
   - `unknown_needs_review`
3. Record whether each file is tracked modified or untracked.
4. Do not move or delete files in this task.

**Verification:**

- Inventory contains every file from `git status --short`.
- No file is classified as unknown without a short reason.

---

### Task 2: Validate imports and test coverage

**Objective:** Ensure every new source module has either a direct test or a documented reason for being script-only/support-only.

**Files:**

- Update: `artifacts/consolidation/file_inventory_2026-06-24.md`

**Steps:**

1. For each `src/factor_lab/market_phenomena_*.py`, find corresponding `tests/test_market_phenomena_*.py` where expected.
2. For each small institutionalization source file, find its test file.
3. For each script, identify the source module it calls.
4. Mark orphan files as `cleanup_candidate` or `needs_test`.

**Verification:**

- No new source file lacks a classification.
- No script lacks a target module or cleanup note.

---

### Task 3: Canonicalize plans

**Objective:** Decide which June 4 plan document is authoritative.

**Files:**

- Keep: `docs/plans/2026-06-04-market-phenomenon-research-agent-plan.md`
- Review: `docs/plans/2026-06-04-phenomenon-first-autonomous-research-agent-plan.md`

**Steps:**

1. Compare both plans for unique accepted requirements.
2. If the second plan is obsolete, archive or remove it in a recoverable way.
3. If it contains unique accepted requirements, merge those into the canonical plan before archiving.
4. Record the decision in the inventory.

**Verification:**

- Exactly one plan is marked canonical.
- The obsolete/archived plan is not needed to understand current implementation.

---

### Task 4: Confirm canonical status consistency

**Objective:** Ensure status documents do not contradict each other.

**Files:**

- `knowledge/small_institutionalization.md`
- `knowledge/market_phenomena_lessons.md`
- `knowledge/market_phenomena_memory.json`
- `artifacts/market_phenomena/human_review_pack.md`
- `artifacts/market_phenomena/strategy_design_approval_gate.md`

**Steps:**

1. Check small institutionalization status says approval is pending and drawdown remains blocked.
2. Check market phenomena status says 20d is review-supported but prototype generation is not approved.
3. Check all queue/live/automation gates remain false.
4. Patch only contradictions, not research outcomes.

**Verification:**

- No status file claims live trading, queue write, broad daemon, or prototype generation is allowed.
- No file claims drawdown blocker is solved.

---

### Task 5: Run narrow verification suites

**Objective:** Prove both workstreams are still test-backed after consolidation edits.

**Commands:**

```bash
pytest tests/test_small_institutionalization_policy.py \
       tests/test_simulated_portfolio_construction_repair.py \
       tests/test_paper_portfolio_weekly_report.py \
       tests/test_small_institutional_operator_pending_consistency_snapshot.py \
       tests/test_small_institutional_operator_pending_observation.py -q
```

```bash
pytest tests/test_market_phenomena_*.py -q
```

**Verification:**

- Both commands pass, or failures are documented with exact failing tests and root-cause notes.

---

### Task 6: Run broad verification if feasible

**Objective:** Detect unintended breakage outside the touched areas.

**Commands:**

```bash
pytest -q
```

Optional, if still present and stable:

```bash
python scripts/smoke_test_factor_lab.py
python scripts/verify_de_openclaw_runtime.py
python scripts/build_queue_explanation.py
```

**Verification:**

- Full suite passes, or any failure is classified as pre-existing / unrelated / caused by consolidation.

---

### Task 7: Prepare review groups

**Objective:** Make the eventual commit/review boundary clear.

**Recommended groups:**

1. `feat: harden small institutionalization approval gates`
2. `feat: add market phenomenon research agent`
3. `docs: consolidate factor lab research status`

**Steps:**

1. Use `git diff --stat` and `git status --short` to verify grouping.
2. Do not commit unless explicitly instructed.
3. Present final changed-file groups and test output.

**Verification:**

- Every changed file belongs to one group or is explicitly listed as cleanup/ignored.

---

## Final Done Criteria

Consolidation is done when:

- Workspace inventory exists and classifies all modified/untracked files.
- Exactly one market phenomenon plan is canonical.
- Small institutionalization remains blocked on drawdown/operator decision, not falsely passed.
- Market phenomenon strategy prototype remains blocked on human review.
- Queue write, broad daemon, automation, automated rerun, production execution, and live trading remain disabled.
- Narrow workstream tests pass.
- Full test status is known.
- The final report separates engineering status from research status.

---

## Current Recommended Verdict

Proceed with consolidation before adding research capability. The system is engineering-healthy enough to checkpoint, but research quality and risk gates are not strong enough to justify more automation.
