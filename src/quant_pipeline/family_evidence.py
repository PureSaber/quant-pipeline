"""Durable cross-study statistical evidence from the immutable trial registry."""

import argparse
from pathlib import Path

import pandas as pd
from quant_lab.family_evidence import collect_family, evaluate_family
from quant_lab.research import canonical, file_hash
from quant_lab.trials import TrialRegistry


def run_family_evidence(
    registry_path,
    family_id,
    output,
    *,
    benchmark_path=None,
    blocks=8,
    block_lengths=(5, 10, 20),
    repetitions=1000,
    seed=17,
):
    values, audit = collect_family(TrialRegistry(Path(registry_path)), family_id)
    benchmark = None
    if benchmark_path:
        frame = pd.read_csv(benchmark_path, index_col=0, parse_dates=True)
        if frame.shape[1] != 1:
            raise ValueError("one explicitly matched benchmark series required")
        benchmark = frame.iloc[:, 0]
    result = evaluate_family(
        values,
        audit,
        benchmark=benchmark,
        blocks=blocks,
        block_lengths=block_lengths,
        repetitions=repetitions,
        seed=seed,
    )
    result["benchmark_sha256"] = file_hash(Path(benchmark_path)) if benchmark_path else None
    with Path(output).open("x", encoding="utf-8") as handle:
        handle.write(canonical(result))
    return result


def nested_research(data, candidates, **settings):
    """Explicit orchestration adapter; fitting/neutralization must live in the fit callback."""
    from quant_lab.nested import nested_selection

    return nested_selection(data, candidates, **settings)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--family-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--benchmark", type=Path)
    parser.add_argument("--blocks", type=int, default=8)
    parser.add_argument("--block-lengths", type=int, nargs="+", default=[5, 10, 20])
    parser.add_argument("--repetitions", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=17)
    args = parser.parse_args()
    run_family_evidence(
        args.registry,
        args.family_id,
        args.output,
        benchmark_path=args.benchmark,
        blocks=args.blocks,
        block_lengths=args.block_lengths,
        repetitions=args.repetitions,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
