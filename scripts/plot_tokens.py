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


def plot_tokens_multiple(
    per_file_tokens: List[Tuple[Path, List[Tuple[str, int]], int]],
    top: int | None,
    out: Path,
    kind: str = "line",
) -> None:
    """Plot token counts for multiple files overlayed (normalized by sum of counts).

    `per_file_tokens` is a list of tuples: (path, tokens_list, line_count).
    `tokens_list` is a list of (label, count) for that file. Normalization is
    done by dividing each token count by the sum of counts in that file.
    """
    # build a mapping per file and compute per-file totals
    file_maps = []  # list of (name, mapping, total_count)
    label_totals = {}
    for path, toks, _line_count in per_file_tokens:
        m = {k: v for k, v in toks}
        total_count = sum(m.values())
        file_maps.append((path.name, m, total_count))
        for k, v in m.items():
            label_totals[k] = label_totals.get(k, 0) + v

    # choose top labels by combined total
    sorted_labels = sorted(label_totals.items(), key=lambda kv: kv[1], reverse=True)
    if top and top > 0:
        sorted_labels = sorted_labels[:top]
    labels = [k for k, _ in sorted_labels]
    if not labels:
        raise SystemExit("No labels found to plot")

    # prepare plotting (horizontal layout)
    plt.style.use("seaborn-v0_8")
    # default sizing: width 10, height scales with number of labels
    default_width = 10
    default_height = max(8, 0.4 * len(labels))
    # allow callers to override via kwargs on the function (kept backwards-compatible)
    # if `fig_width` and `fig_height` attributes are passed in via outer scope,
    # they will be used by the caller (see `main()` where CLI args are forwarded).
    width = getattr(plot_tokens_multiple, "_fig_width", None) or default_width
    height = getattr(plot_tokens_multiple, "_fig_height", None) or default_height
    fig, ax = plt.subplots(figsize=(width, height))

    # We'll plot counts on the x-axis and labels on the y-axis so the chart is
    # horizontal and easier to read for long label names.
    y = list(range(len(labels)))

    if kind == "line":
        for name, m, total_count in file_maps:
            denom = total_count if total_count > 0 else 1
            counts = [m.get(lbl, 0) / denom for lbl in labels]
            ax.plot(counts, y, marker="o", label=name)
        ax.set_yticks(y)
        ax.set_yticklabels(labels)
        ax.set_xlabel("Fraction of tokens (per file)")
    elif kind == "bar":
        # overlay horizontal bars at the same y positions with transparency so
        # files can be compared visually. Each file's series is normalized by
        # its total count.
        height = 0.8
        for i, (name, m, total_count) in enumerate(file_maps):
            denom = total_count if total_count > 0 else 1
            counts = [m.get(lbl, 0) / denom for lbl in labels]
            ax.barh(y, counts, height=height, alpha=0.5, label=name)
        ax.set_yticks(y)
        ax.set_yticklabels(labels)
        ax.set_xlabel("Fraction of tokens (per origin)")
    else:
        raise ValueError("Unknown plot kind: use 'bar' or 'line'")

    ax.set_title("Token counts (normalized across origins)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print(f"Saved plot to: {out}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot tokens from one or more token-count text files")
    parser.add_argument("--inputs", "-i", type=Path, nargs="+", help="one or more input text files (space separated)")
    parser.add_argument("--top", type=int, default=20, help="top-N tokens to plot (default 20), use 0 or -1 for all")
    parser.add_argument("--out", "-o", type=Path, default=Path("tokens.png"), help="output image path")
    parser.add_argument("--kind", choices=("bar","line"), default="bar", help="plot kind (bar overlays recommended for multiple files)")
    parser.add_argument("--fig-width", type=float, default=None, help="figure width in inches (overrides default)")
    parser.add_argument("--fig-height", type=float, default=None, help="figure height in inches (overrides default)")
    args = parser.parse_args()

    inputs = args.inputs or [Path("all_tokens.txt")]
    for p in inputs:
        if not p.exists():
            raise SystemExit(f"Input file not found: {p}")

    per_file = []
    for p in inputs:
        toks = parse_token_file(p)
        # sort descending
        toks.sort(key=lambda t: t[1], reverse=True)
        # count lines for normalization
        with p.open("r", encoding="utf-8") as fh:
            line_count = sum(1 for _ in fh)
        per_file.append((p, toks, line_count))

    top = args.top
    if top is None or top <= 0:
        top = None

    # forward fig size to plotting function via attributes (keeps simple call signature)
    if args.fig_width is not None:
        setattr(plot_tokens_multiple, "_fig_width", args.fig_width)
    if args.fig_height is not None:
        setattr(plot_tokens_multiple, "_fig_height", args.fig_height)
    plot_tokens_multiple(per_file, top, args.out, kind=args.kind)


if __name__ == "__main__":
    main()
