#!/usr/bin/env python3
"""Convert a text file with blocks separated by a 100-hyphen line into a JSONL
file compatible with `ast_parsing/main.py`.

Behavior:
- Split input on lines that contain at least 100 '-' characters.
- Remove preamble lines that start with 'Processing' or 'Wrote' (case-insensitive).
- For each non-empty block, emit a JSON object with the shape:
    {
      "repo_name": <provided or null>,
      "files": [
         {"path": <provided or "<unknown>" >, "udfs": [], "df_exprs": [<block_text>]}
      ]
    }

The script attempts to extract a repo name or file path from the block using simple
heuristics. Use `--repo` or `--path` to force values when heuristics fail.

Usage:
  python scripts/convert_delimited_to_jsonl.py -i translations_1_22.txt -o snippets.jsonl

"""
from __future__ import annotations
import re
import json
import argparse
from pathlib import Path
from typing import List

BLOCK_SPLIT_RE = re.compile(r"(?m)^-{100,}\s*$")
PREAMBLE_RE = re.compile(r"(?i)^(processing|wrote)\b.*$")
REPO_JSON_RE = re.compile(r'"repo_name"\s*:\s*"([^"]+)"')
PATH_RE = re.compile(r'([Ff]ile[:=]\s*|path\s*[:=]\s*)(?P<path>\S+)')


def normalize_block(block: str) -> str:
    lines = [l.rstrip() for l in block.splitlines()]
    # drop preamble lines
    lines = [l for l in lines if not PREAMBLE_RE.match(l.strip())]
    # drop leading/trailing empty lines
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines).strip()


def extract_after_semicolon(block: str) -> str:
    """Return the substring after the first semicolon in `block`.

    If no semicolon is found, return the original block. Leading/trailing
    whitespace is stripped.
    """
    idx = block.find(";")
    if idx == -1:
        return block.strip()
    # everything after the first semicolon
    return block[idx + 1 :].strip()


def guess_repo_and_path(block: str):
    repo = None
    path = None
    # try to find JSON-style repo_name
    m = REPO_JSON_RE.search(block)
    if m:
        repo = m.group(1)
    # try to find a file path
    m2 = PATH_RE.search(block)
    if m2:
        path = m2.group('path')
    return repo, path


def convert(
    input_path: Path,
    out_path: Path,
    repo_override: str | None = None,
    path_override: str | None = None,
    after_semicolon: bool = False,
):
    text = input_path.read_text(encoding="utf-8")
    parts = [p for p in BLOCK_SPLIT_RE.split(text)]

    written = 0
    with out_path.open("w", encoding="utf-8") as outf:
        for part in parts:
            norm = normalize_block(part)
            if not norm:
                continue
            repo_guess, path_guess = guess_repo_and_path(norm)
            repo = repo_override or repo_guess or None
            path = path_override or path_guess or "<unknown>"

            snippet = extract_after_semicolon(norm) if after_semicolon else norm

            record = {
                "repo_name": repo,
                "files": [
                    {
                        "path": path,
                        "udfs": [],
                        "df_exprs": [snippet],
                    }
                ],
            }
            outf.write(json.dumps(record, ensure_ascii=False) + "\n")
            written += 1
    print(f"Wrote {out_path} with converted {written} blocks (empty blocks skipped).")


def main():
    parser = argparse.ArgumentParser(description='Convert delimited text to JSONL compatible with main.py')
    parser.add_argument('-i', '--input', type=Path, required=True)
    parser.add_argument('-o', '--output', type=Path, required=True)
    parser.add_argument('--repo', help='Optional repo_name to set on all records')
    parser.add_argument('--path', help='Optional file path to set on all records')
    parser.add_argument('--after-semicolon', action='store_true', help='Extract only the text after the first semicolon in each block')
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f'Input not found: {args.input}')

    convert(
        args.input,
        args.output,
        repo_override=args.repo,
        path_override=args.path,
        after_semicolon=args.after_semicolon,
    )


if __name__ == '__main__':
    main()
