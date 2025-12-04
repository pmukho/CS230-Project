from collections import Counter, defaultdict
import json
from pathlib import Path
from grammar_adapter import GrammarAdapter
from ast_parsing import try_parse, SparkCallVisitor
from object_types import UDFInfo

adapter = GrammarAdapter()

def merge_meta_into_visitor(visitor: SparkCallVisitor, meta: dict):
    #I can try merge scraped third-party call metadata
    for call in meta.get("calls", []) if meta else []:
        lib = call.get("library")
        if lib:
            visitor.third_party_libs.add(lib)
            visitor.third_party_lib_freq[lib] += 1

    udf_name = meta.get("udf_name") if meta else None
    if udf_name:
        if udf_name not in visitor.udfs:
            visitor.udfs[udf_name] = UDFInfo(name=udf_name, snippet=meta.get("snippet",""))
        #I need to attach any calls to the UDF
        for call in meta.get("calls", []) or []:
            lib = call.get("library")
            if lib:
                visitor.udfs[udf_name].third_party_dependencies.add(lib)

def analyze_snippet(snippet: str, meta: dict) -> Counter:
    tree = try_parse(snippet)
    if not tree:
        return Counter()
    visitor = SparkCallVisitor()
    tree.visit(visitor)
    #Optional merge
    if isinstance(meta, dict):
        merge_meta_into_visitor(visitor, meta)
    feats = adapter.features_from_visitor(visitor)
    return feats

def run_on_file(input_path: Path, out_jsonl: Path):
    #TODO: decide our aggregators
    per_file = defaultdict(Counter) #this is path to Counter.
    per_repo = defaultdict(Counter) #this one is repo to Counter.
    global_counter = Counter()

    with input_path.open("r", encoding="utf-8") as inf, out_jsonl.open("w", encoding="utf-8") as outf:
        for line in inf:
            if not line.strip():
                continue
            entry = json.loads(line)
            repo_name = entry.get("repo_name")
            for file_obj in entry.get("files", []) or []:
                path = file_obj.get("path", "<unknown>")
                #iterate snippet sources: udfs + df_exprs to keep snippet level provenance
                for snippet in (file_obj.get("df_exprs", []) or []):
                    meta = {"path": path, "repo_name": repo_name}
                    feats = analyze_snippet(snippet, meta)
                    out_record = {
                        "repo_name": repo_name,
                        "path": path,
                        "snippet": snippet if len(snippet) < 2000 else snippet[:2000], #TODO: cut for now.
                        "features": dict(feats)
                    }
                    outf.write(json.dumps(out_record, ensure_ascii=False) + "\n")

                    #aggs.
                    per_file[path].update(feats)
                    per_repo[repo_name].update(feats)
                    global_counter.update(feats)

    return {"per_file": per_file, "per_repo": per_repo, "global": global_counter}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Extract grammar features from JSONL of snippets")
    parser.add_argument("-i", "--input", type=Path, default=Path(__file__).parent / "sample_input_typed.jsonl",
                        help="input JSONL file (default: sample_input_typed.jsonl in this folder)")
    parser.add_argument("-o", "--output", type=Path, default=Path(__file__).parent / "snippet_features.jsonl",
                        help="output JSONL file for per-snippet features")
    parser.add_argument("--top", type=int, default=None, help="show top-N global tokens after run (default: uncapped/all)")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Input file not found: {args.input}")
        raise SystemExit(1)

    print(f"Processing {args.input} -> {args.output} ...")
    results = run_on_file(args.input, args.output)
    print(f"Wrote snippet features to: {args.output}")

    top = results["global"].most_common(args.top)
    if args.top is None:
        print("Top (all) global tokens:")
    else:
        print(f"Top {args.top} global tokens:")
    for tok, cnt in top:
        print(f"  {tok}: {cnt}")