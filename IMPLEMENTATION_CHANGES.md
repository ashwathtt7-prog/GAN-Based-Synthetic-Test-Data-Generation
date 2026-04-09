# Implementation Changes

This file tracks the local changes made after the previously pushed baseline.

## Frontend

- Added a richer generation insights flow in the dashboard:
  - tier routing visibility
  - training metrics visibility
  - activity log visibility
- Added source-vs-generated compare support in the data viewer.
- Wired the frontend to use the local backend endpoint currently being tested on port `8001`.

## Backend API

- Added training telemetry endpoint:
  - `GET /api/training-metrics`
- Added run resolution helper logic so the frontend can inspect the latest run automatically.
- Extended pipeline start request with `fast_mode`.
- Kept the existing generated-data and source-data viewer endpoints in place for compare mode.

## Orchestrator

- Added shared table profiling:
  - structural columns
  - modeled columns
  - sensitive columns
  - fingerprint
- Added generated-tier tracking for better frontend visibility.
- Added model registry reuse flow:
  - exact fingerprint reuse
  - near-match compatibility scoring
  - shortened adaptation training for near matches
- Added shared modeled-frame preparation so numeric/date business columns stay learnable instead of being turned into masked string categories.
- Added shared repair hooks before validation:
  - FK stitching
  - allowed-value repair
  - temporal repair
  - identifier uniqueness repair
- Added training lifecycle logging:
  - `training_start`
  - `training_metric`
  - `training_complete`
  - `training_failed`
  - `model_reuse`
  - `generation_failed`

## ML Runtime

- Added `synthesis/sdv_runtime.py` to force safer local SDV/CTGAN behavior:
  - synchronous transformer preprocessing
  - reduced BLAS/OpenMP thread pressure
- Reduced CTGAN/TVAE memory footprint:
  - lower batch sizes
  - lower latent/network dimensions
  - CPU-only execution for local stability
- Fixed SDV metadata overrides to match the installed library behavior.
- Added primary-key sanitization so SDV does not auto-promote non-ID columns into invalid PKs.
- Made the training monitor tolerant to CTGAN loss-column naming differences across versions.

## Routing and Profiling

- Updated routing so structural-heavy tables are not forced into ML if they have no meaningful modeled slice.
- Updated profiling so high-cardinality masked identifier/text columns stay out of the ML path.
- Kept numeric business metrics eligible for CTGAN/TVAE even when related columns are masked.

## Validation

- Prevented temporal validation from crashing on malformed datetime strings.
- Restricted uniqueness repair so temporal fields are not rewritten like identifiers.
- Reduced noisy privacy failures by skipping exact-overlap checks on structural IDs, flags, codes, and date-style columns.
- Reduced re-identification runtime by using a narrower validation column set and smaller comparison samples.

## Pretraining

- Added standalone pretraining script:
  - `scripts/pretrain_models.py`
- This script builds reusable baseline models intentionally, separate from a normal pipeline run.
- Verified local baseline training on:
  - `CUST_MSTR` via CTGAN
  - `INVC` via CTGAN
  - `NTWK_ELEM` via TVAE
  - `CDR_REC` via CTGAN
  - `INVC_LN_ITEM` via CTGAN

## Current Verification State

- Training telemetry is now visible for real runs.
- Exact model reuse is working for cached tables.
- End-to-end verification has now been run on three dataset slices from the telecom source graph:
  - Small: `INVC` only, run `41f78bbf-e341-48ea-800a-580c4af5ef80`, completed in about `20.5s`
  - Medium: `CUST_MSTR`, `CUST_ADDR`, `CUST_CNTCT`, `IDENT_DOC`, `BLNG_ACCT`, run `a0887d40-eb10-4a3c-9e1c-8cdf7f26e388`, completed in about `192.4s`
  - Large connected graph: 19 related customer/billing tables, run `6dba20da-00ff-42cb-9290-f64bd85cb843`, completed in about `229.8s`
- The latest large run generated all requested tables with source-matching row counts.
- Large-run validation noise dropped materially after tier-aware validator changes:
  - medium slice: `39 / 121` failed checks
  - large connected slice: `64 / 356` failed checks
- Additional hardening added during verification:
  - normalized scalar/string/range constraint values before allowed-value repair
  - fixed rule-based generation fallback for sparse masked text columns
  - forced large structural-heavy tables like `SUBSCR_ACCT` onto deterministic generation
  - made training-metrics API responses JSON-safe when metrics contain `NaN`
- Nothing from this pass has been pushed.

## Remaining Risks

- Some rule-based tables still fail a handful of business/privacy checks and need another quality pass:
  - notable tables in the large run include `FIELD_AGT`, `WRK_ORD_ASSGN`, `BLNG_ACCT`, and `SVC_ORD`
- The graph visualization redesign has not been done yet.
- The current large-suite benchmark is on the telecom relational graph in this repo; cross-domain benchmarking still needs a second and third source dataset if you want broader generalization evidence.
