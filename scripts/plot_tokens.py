#!/usr/bin/env python3
"""Plot token counts from a text file like `all_tokens.txt`.

The file is expected to contain lines like:
  <label>: <count>
Possibly preceded by header text. The script extracts numeric counts and
plots either a horizontal bar chart (default) or a line plot.

Usage:
  python scripts/plot_tokens.py --input all_tokens.txt --top 20 --out tokens.png

"""
from __future__ import annotations
import re
import argparse
from pathlib import Path
from typing import List, Tuple

import matplotlib.pyplot as plt

TOKEN_LINE_RE = re.compile(r"^\s*(?P<label>.+?):\s*(?P<count>\d+)\s*$")


def parse_token_file(path: Path) -> List[Tuple[str, int]]:
    tokens = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            m = TOKEN_LINE_RE.match(line)
            if not m:
                continue
            label = m.group("label").strip()
            # normalize: drop common prefixes like DF_OP:: for readability
            if label.startswith("DF_OP::"):
                label = label.split("::", 1)[1]
            count = int(m.group("count"))
            tokens.append((label, count))
    return tokens


def plot_tokens(tokens: List[Tuple[str, int]], top: int | None, out: Path, kind: str = "bar") -> None:
    if top is not None:
        tokens = tokens[:top]

    if not tokens:
        raise SystemExit("No token data found to plot")

    labels, counts = zip(*tokens)

    # create a horizontal bar chart for readability
    plt.style.use("seaborn-v0_8")
    fig, ax = plt.subplots(figsize=(10, max(4, 0.25 * len(labels))))

    if kind == "bar":
        y_positions = range(len(labels)-1, -1, -1)  # reverse so largest is on top
        ax.barh(y_positions, counts[::-1], align="center")
        ax.set_yticks(y_positions)
        ax.set_yticklabels(labels[::-1])
        ax.set_xlabel("Count")
    elif kind == "line":
        ax.plot(range(len(counts)), counts, marker="o")
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=45, ha="right")
        ax.set_ylabel("Count")
    else:
        raise ValueError("Unknown plot kind: use 'bar' or 'line'")

    ax.set_title("Token counts")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print(f"Saved plot to: {out}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot tokens from a token-count text file")
    parser.add_argument("--input", "-i", type=Path, default=Path("all_tokens.txt"), help="input text file")
    parser.add_argument("--top", type=int, default=20, help="top-N tokens to plot (default 20), use 0 or -1 for all")
    parser.add_argument("--out", "-o", type=Path, default=Path("tokens.png"), help="output image path")
    parser.add_argument("--kind", choices=("bar","line"), default="bar", help="plot kind")
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input file not found: {args.input}")

    tokens = parse_token_file(args.input)
    # sort descending
    tokens.sort(key=lambda t: t[1], reverse=True)

    top = args.top
    if top is None or top <= 0:
        top = None

    plot_tokens(tokens, top, args.out, kind=args.kind)


if __name__ == "__main__":
    main()
