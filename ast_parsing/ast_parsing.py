import json
import libcst as cst
from pathlib import Path


def extract_snippets(filename):
    """Returns (repo_name, snippet) tuples from the jsonl all_results file."""
    with open(filename, encoding="utf-8") as f:
        #print("Opened file:", filename)
        for line in f:
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
                repo_name = entry.get("repo_name", "<unknown>")
                #print("Processing repo:", repo_name)
                for match in entry.get("matches", []):
                    #print("detected match, processing...")
                    snippet = match.get("snippet", "").strip()
                    if snippet:
                        #print("GRIDDLE", repo_name, "YAYAYA", snippet)
                        yield repo_name, snippet
            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON line: {e}")


def try_parse(snippet):
    """Try parsing a code snippet using libcst. TODO: check if we want to use a different library, but this seems pretty robust for our usecase so far."""
    try:
        # DOC says: parse_expression() works best for single expressions and parse_module() for blocks
        node = cst.parse_expression(snippet)
        #print("Parsed as expression")
        return node
    except Exception:
        try:
            node = cst.parse_module(snippet)
            #print("Parsed as module")
            return node
        except Exception:
            #print("Failed to parse snippet")
            return None


class SparkCallVisitor(cst.CSTVisitor):
    """Collects DataFrame operations and UDF usages."""
    def __init__(self):
        self.funcs = set()
        self.third_party_libs = set()
        self.has_udf = False

    def visit_Attribute(self, node):
        name = node.attr.value
        if name in {"filter", "select", "alias", "withColumn", "join"}:
            self.funcs.add(name)

    #find udfs
    def visit_Call(self, node):
        code = node.func.code if hasattr(node.func, "code") else str(node.func)
        if "udf" in code:
            self.has_udf = True

    #TODO: define what third party libraries we actually care abt.
    def visit_Import(self, node):
        for alias in node.names:
            name = alias.name.value
            if name in {"numpy", "pandas", "torch", "sklearn"}: 
                self.third_party_libs.add(name)

    def visit_ImportFrom(self, node):
        if node.module and node.module.value in {"numpy", "pandas", "torch", "sklearn"}:
            self.third_party_libs.add(node.module.value)


def analyze_file(filename):
    """Parse JSONL and analyze each snippet."""
    for repo, snippet in extract_snippets(filename):
        tree = try_parse(snippet)
        if not tree:
            continue

        visitor = SparkCallVisitor()
        tree.visit(visitor)

        if visitor.funcs or visitor.has_udf or visitor.third_party_libs:
            print(f"\nFOLDER {repo}")
            print(f"Snippet: {snippet}")
            print(f"- PySpark Ops: {sorted(visitor.funcs) or '—'}")
            print(f"- Uses UDF: {visitor.has_udf}")
            print(f"- Third-party libs: {sorted(visitor.third_party_libs) or '—'}")


if __name__ == "__main__":
    #filename = Path(__file__).parent.parent / "scraping" / "results" / "all_results.jsonl"
    filename = Path(__file__).parent.parent / "ast_parsing" / "sample_results.jsonl"
    analyze_file(filename)