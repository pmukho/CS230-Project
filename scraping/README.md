# Scraping for realistic PySpark workloads from Github

## Prereqs

- Python 3.10.18.

## Usage

1. Create `.env` file in project root `.` or under `./scraping`.
3. Create fine-grained token on GitHub with access to public repositories (should be default permissions).
4. Add the following line to the `.env` file:
   `GITHUB_TOKEN=YOUR_TOKEN_VALUE_HERE`
   DO NOT use any spaces or braces or you will run into 401 permission error and need to clear kernel.
5. Run the following lines before using the notebook:
   ```
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
6. Run all cells and expect results to be stored in `./scraping/results/summary.jsonl`

## Scraping Output Explained

Output is stored in `./scraping/results/summary.jsonl` where each line is a valid JSON object matching the schema given below. Tools like `jq` will make output file easy for human to read.

```json
{
  repo_name: string,
  clone_url: string (URL),
  files: [
    {
      path: string,
      udfs: [
        {
          name: string,
          definition: string
        }
      ],
      df_exprs: [string]
    }
  ]
}
```
