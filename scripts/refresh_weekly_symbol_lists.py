from __future__ import annotations

import argparse
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "weekly-options-over5.txt"
DEFAULT_BASELINE = ROOT / "weekly-options-over5-median80-mintrades70.txt"
DEFAULT_MINUS = ROOT / "weekly-options-over5-minus-median80-mintrades70.txt"
DEFAULT_PART_PREFIX = ROOT / "weekly-options-over5-minus-median80-mintrades70-part"
DEFAULT_PART_COUNT = 5


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh the derived weekly symbol lists by subtracting the baseline universe "
            "from the source list and repartitioning the remainder into equal sequential chunks."
        )
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--minus-output", type=Path, default=DEFAULT_MINUS)
    parser.add_argument("--part-prefix", dest="part_prefix", type=Path, default=DEFAULT_PART_PREFIX)
    parser.add_argument("--part-count", type=int, default=DEFAULT_PART_COUNT)
    return parser


def _read_symbols(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_symbols(path: Path, symbols: list[str]) -> None:
    path.write_text("\n".join(symbols) + "\n", encoding="utf-8")


def _chunk_sizes(total: int, part_count: int) -> list[int]:
    base_size, remainder = divmod(total, part_count)
    return [base_size + (1 if index < remainder else 0) for index in range(part_count)]


def main() -> int:
    args = build_parser().parse_args()
    if args.part_count <= 0:
        raise SystemExit("--part-count must be >= 1")

    source_symbols = _read_symbols(args.source)
    baseline_symbols = set(_read_symbols(args.baseline))
    minus_symbols = [symbol for symbol in source_symbols if symbol not in baseline_symbols]
    _write_symbols(args.minus_output, minus_symbols)

    sizes = _chunk_sizes(len(minus_symbols), args.part_count)
    cursor = 0
    for part_index, size in enumerate(sizes, start=1):
        chunk = minus_symbols[cursor : cursor + size]
        cursor += size
        _write_symbols(Path(f"{args.part_prefix}{part_index}.txt"), chunk)

    print(f"baseline_count={len(baseline_symbols)}")
    print(f"minus_count={len(minus_symbols)}")
    print(f"part_sizes={','.join(str(size) for size in sizes)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
