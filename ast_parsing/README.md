# Transforming Scraped Data into AST Trees and Parsing It

## Prereqs

1. Python 3.10 (tested with 3.10.18)

## Usage

1. Make sure cwd matches the directory containing this file.
2. Run the following lines before running the ast_parsing script:
   ```
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. Run the ast_parsing.py script and expect results to be stored in `.results/ast_parsing_results.jsonl`

# AST Parsing and Grammar Features

This folder contains utilities to parse PySpark snippets into AST/CST, extract features, and convert them into a simple grammar/op-count representation suitable for aggregation and analysis.

## How to Run

### 1) Parse and summarize snippets (`ast_parsing.py`)

`ast_parsing.py` reads a JSONL file of scraped snippets and prints per-snippet analysis results and a small summary demo.

Run with the default sample input:

```powershell
cd into the `ast_parsing` folder then,
python .\ast_parsing.py
```

Run with a specific JSONL file:

```powershell
python .\ast_parsing.py .\sample_input_typed.jsonl
```

Output:

- Prints the number of analysis results found and up to 20 `AnalysisResult` entries (repo/path/snippet + extracted ops/libs/UDFs).
- Prints a short demo of `SparkSnippetSummary` for up to 5 snippets.

Input format (JSONL): each line contains `repo_name`, `clone_url`, and `files` with per-file `udfs` and `df_exprs`. `extract_snippets` handles multiple UDF body keys (`def`, `definition`, `code`, `function`).

### 2) Generate grammar features (`main.py` + `grammar_adapter.py`)

`main.py` processes the same JSONL and writes per-snippet grammar features to a JSONL, while also printing aggregated token counts.

Run on the sample input, choose an output file of your choice, and show top-N tokens:

```powershell
cd into the `ast_parsing` folder then,
python .\main.py -i .\sample_input_typed.jsonl -o .\snippet_features.jsonl --top 25
```

Outputs:

- `snippet_features.jsonl`: one line per snippet `{repo_name, path, snippet, features}` where `features` is a token→count dict.
- Console print: top-N tokens aggregated across the input.

## File Summaries

### `ast_parsing.py`

- Loads snippets via `extract_snippets` from JSONL (`udfs`, `df_exprs`).
- Parses each snippet with LibCST (`try_parse`).
- Traverses with `SparkCallVisitor` to collect:
  - `pyspark_ops`: DataFrame operators (e.g., `select`, `withColumn`, `groupBy`, `distinct`).
  - `third_party_libs`: imports seen in the snippet (e.g., `numpy`, `pandas`).
  - `udfs`: `UDFInfo` per UDF (decorator, applied_to_df, registered_sql, third_party_dependencies).
- Emits `AnalysisResult` per snippet (repo/path/snippet + extracted fields) and prints a small demo of `SparkSnippetSummary`.

### `grammar_adapter.py`

- Converts a `SparkSnippetSummary` (or visitor summary) into a `Counter` of grammar tokens:
  - `DF_OP::<op>` — one per DataFrame operator.
  - `DF_THIRD_PARTY_LIB::<lib>` — one per third‑party library.
  - `UDF_DEF`, `DF_UDF_MAP`, `UDF_NAME::<name>` — presence and identity of UDFs.
  - `UDF_THIRD_PARTY_LIB::<lib>` — libraries referenced inside UDFs.
- Used by `main.py` to produce per-snippet features and global aggregates.

### `main.py`

- Orchestrates end‑to‑end feature extraction:
  - Reads JSONL, iterates snippets (keeps snippet-level provenance).
  - For each snippet: parse → visit → adapter → `features` dict.
  - Writes per-snippet JSONL records and aggregates `per_file`, `per_repo`, and `global` counters.
  - Prints top‑N or all global tokens based on `--top`.

## How They Interact

- `ast_parsing.py` provides parsing and visiting logic (`SparkCallVisitor`) and the compact summary (`SparkSnippetSummary`).
- `grammar_adapter.py` turns the summary into a simple, aggregable vocabulary of grammar tokens.
- `main.py` wires them together for batch processing and outputs both per-snippet features and corpus-level aggregates.

## Notes

# Analysis Output Format

Each line in the `jsonl` output is matched like the schema given below

```json
{
  folder: string,
  snippet: string,
  pyspark_ops: [string],
  uses_udf: boolean,
  third_party_libs: [string]
}
```
