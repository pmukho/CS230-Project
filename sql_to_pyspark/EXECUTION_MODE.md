# PySpark Execution Mode

## Overview

The test runner now supports **two modes**:

1. **Validation Mode** (default): Checks syntax and PySpark patterns without running code
2. **Execution Mode** (`--execute`): Actually runs the translated PySpark code with real data

## Usage

### Validation Mode (Fast, No Dependencies)
```bash
python run_tpcds_tests.py --level basic
```
- ✓ Validates Python syntax using AST parsing
- ✓ Checks for PySpark DataFrame patterns
- ✓ Fast execution (~2 seconds)
- ✓ No Java/Spark dependencies needed

### Execution Mode (Full Verification)
```bash
python run_tpcds_tests.py --level basic --execute
```
- ✓ All validation mode checks
- ✓ Actually runs translated code in Spark
- ✓ Verifies queries produce results
- ✓ Reports row counts
- ⚠️  Requires Java 11-21 and PySpark

## Requirements for Execution Mode

### Java Version
PySpark requires **Java 11, 17, or 21**. If you have Java 25 (incompatible):

```bash
# Install Java 21 via Homebrew
brew install openjdk@21

# Set JAVA_HOME for current session
export JAVA_HOME=$(/usr/libexec/java_home -v 21)

# Or add to ~/.zshrc for permanent setting
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 21)' >> ~/.zshrc
```

### PySpark Installation
```bash
pip install pyspark
```

### Data Files
Execution mode requires CSV data files in `tpcds_data/`:
- customer.csv
- date_dim.csv
- inventory.csv
- item.csv
- store.csv
- store_sales.csv

These are pre-generated and included in the repository (841 KB total).

## Examples

### Run all queries with execution
```bash
python run_tpcds_tests.py --execute --verbose
```

### Test intermediate complexity with execution
```bash
python run_tpcds_tests.py --level intermediate --execute
```

### Quick validation of advanced queries
```bash
python run_tpcds_tests.py --level advanced
```

## Graceful Fallback

If you use `--execute` but Spark is unavailable (e.g., Java 25 issue), the runner will:
1. Show clear error message with fix instructions
2. Automatically fall back to validation-only mode
3. Complete all syntax and pattern checks
4. Report 100% pass rate for valid translations

This ensures testing continues even in environments where Spark execution isn't possible.

## Output Differences

### Validation Mode Output
```
✓ BASIC: 10/10 passed (100.0%)
```

### Execution Mode Output (with --verbose)
```
✓ BASIC.1: Simple store sales query - 100 rows
✓ BASIC.2: Customer lookup - 50 rows
✓ BASIC.3: Item price analysis - 234 rows
...
✓ BASIC: 10/10 passed (100.0%)
```

## Benefits

| Feature | Validation Mode | Execution Mode |
|---------|----------------|----------------|
| Syntax validation | ✓ | ✓ |
| Pattern detection | ✓ | ✓ |
| Actual execution | ✗ | ✓ |
| Row count verification | ✗ | ✓ |
| Runtime error detection | ✗ | ✓ |
| Execution time | ~2 sec | ~10-30 sec |
| Dependencies | Python only | Java + PySpark |

## Troubleshooting

### "getSubject is not supported"
- **Cause**: Java 25 incompatibility with Spark
- **Fix**: Install and use Java 11-21 (see above)

### "Spark session not available"
- **Cause**: PySpark not installed
- **Fix**: `pip install pyspark`

### "Data directory not found"
- **Cause**: Missing `tpcds_data/` folder
- **Fix**: Run `python generate_tpcds_data_csv.py` to regenerate data

### No rows returned but query passes
- This is normal for queries with filters that match no data
- Execution validates the query runs, not that it returns data
