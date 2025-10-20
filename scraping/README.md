# Scraping for realistic PySpark workloads from Github

## Assumptions
1. Python 3.10 (tested with 3.10.18)

## Usage
1. Make sure cwd matches the directory containing this file.
2. Create `.env` file in either cwd or one of its parent directories.
3. Create fine-grained token on GitHub with access to public repositories.
4. Add the following line to the `.env` file:
    `GITHUB_TOKEN={token value}`
5. Run the following lines before using the notebook:
    ```
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```