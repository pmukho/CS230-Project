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

# Analysis Output Explained

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