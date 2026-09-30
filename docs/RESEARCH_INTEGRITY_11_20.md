# Research integrity: family evidence and continuous OOS

Install the frozen research environment from `requirements-research.lock`; base-only
`requirements.lock` does not install the equity/report adapters. See quant-lab's
`docs/RESEARCH_INTEGRITY_11_20.md` for equations, assumptions and objective units.

Rebuild locks with pip-tools on Python 3.10, then install and check the complete closure:

```sh
python -m piptools compile --extra=dev --build-deps-for=editable --allow-unsafe --strip-extras --index-url=https://pypi.org/simple --output-file=requirements.lock pyproject.toml
python -m piptools compile --extra=dev --extra=research --build-deps-for=editable --allow-unsafe --strip-extras --index-url=https://pypi.org/simple --constraint=requirements-research.in --output-file=requirements-research.lock pyproject.toml
python -m pip install --no-deps -r requirements-research.lock
python -m pip install --no-deps --no-build-isolation -e .
python -m pip check
```

The research input preserves the audited AKShare metadata-only derivative by immutable
wheel URL and SHA256; it does not require a sibling checkout's vendor directory.
The report adapter's exact public pins intentionally also constrain this combined environment.

## One family across studies (11–14)

Register `TrialRegistry.register_family` **before** its member studies. Recipes carry the same
`family_id` and `measurement_basis`. Run members with an explicit shared database:

```sh
quant-research recipe.yaml --output runs/member-a --registry research-family.db
quant-family-evidence --registry research-family.db --family-id momentum-v1 \
  --benchmark cash-zero.csv --blocks 8 --block-lengths 5 10 20 --repetitions 2000 \
  --seed 17 --output family-evidence.json
```

The benchmark file has one simple net-return column and the exact candidate date index.
Its currency, benchmark identity and economic convention must match the registered basis;
the supplied file hash is recorded. Results never inner-join away missing days or failed
attempts. Without an explicit benchmark DSR/SPA cannot be interpreted as excess-return
evidence. The output shows method unavailability and every block-length sensitivity.

`paired_bootstrap(values, method="stationary", block_lengths=[5,10,20])` adds dependency
sensitivity plus HAC. The old circular sqrt(T) call remains available for reproducibility;
existing FDR summaries are not silently changed to a new p-value selector.

## Continuous OOS (15)

Create a **new study ID** and set:

```yaml
validation:
  train_sessions: 126
  test_sessions: 21
  embargo_sessions: 5
  selection_metric: total_return
  account_policy: continuous
```

The default remains `independent`: each test fold starts from cash. `continuous` freezes
training decisions, then executes one OOS ledger with uninterrupted cash, holdings, settlement,
orders and risk latches. Parameters change only future scheduled decisions. Fee policy, risk
policy, initial capital and static-vs-current-NAV allocation mode cannot change mid-path.
Boundary switching does not force an extra rebalance. Outstanding orders retain their terms.

Every candidate has a continuous path. A train-selected sequence of *different candidates*
is replayed again as one real account, rather than splicing returns from independently funded
candidate accounts. The selected-path request/artifact hashes support verified repeat reads.
An interrupted selected-path directory is kept for investigation; use a new output directory
to retry after diagnosis. This is a deterministic batch run, not durable live checkpoint/resume.

Inspect `selection.json`, `validation.json`, `continuous/`, `selected-continuous/`,
`research.html` and `validation.html`. Training reports retain their diagnostics; a continuous
mixed-factor path does not claim the first candidate's factor IC describes the whole path.

## Nested selection and market transfer (18)

`family_evidence.nested_research` delegates the bounded chronological nested protocol in
quant-lab. All learned directions, windows, neutralization and model choices belong in its
inner `fit` callback. This is an explicit Python API; setting `account_policy: continuous`
alone does **not** enable nested tuning. Nested scores are not funded-account returns.
Persist the returned recipe/inner-fold/selection hashes with the experiment. Transfer plans
must be registered with fixed source parameters and target currency, costs and trading rules.

Tests distinguish these estimands, perturb outer labels, compare unchanged segment runs to
one-pass ledgers, retain failures and reject tampered resume artifacts. No live orders are sent.
