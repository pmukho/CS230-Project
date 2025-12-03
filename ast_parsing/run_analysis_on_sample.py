"""
Test runner: read sample_input.jsonl, run analyze_file from ast_parsing.py,
and write AnalysisResult objects to analysis_results.jsonl (JSONL).
"""

import os

from ast_parsing import analyze_file

SAMPLE_PATH = "scraping/results/summary-useful-200.jsonl"
OUT_PATH = "results/analysis_results.jsonl"

def write_results_jsonl(results, out_path):
    """Append AnalysisResult objects (or dicts) to out_path as JSONL."""
    # ensure directory exists
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for r in results:
            # r may be an AnalysisResult dataclass with to_json/to_dict, or a dict
            if hasattr(r, "to_json"):
                line = r.to_json()
            else:
                import json
                line = json.dumps(r, ensure_ascii=False)
            f.write(line + "\n")

def main():
    print(f"Reading sample input: {SAMPLE_PATH}")
    results = analyze_file(SAMPLE_PATH)  # returns List[AnalysisResult]

    print(f"Found {len(results)} analysis results. Writing to: {OUT_PATH}")
    write_results_jsonl(results, OUT_PATH)

if __name__ == "__main__":
    main()