#!/usr/bin/env python3
"""
TPC-DS SQL-to-PySpark Translator Test Runner

Tests the translator against TPC-DS schema queries at various complexity levels.
Verifies:
1. Translation produces valid Python code
2. Translated code is syntactically correct
3. Contains appropriate PySpark patterns
4. Executes successfully and produces results

Usage:
    python run_tpcds_tests.py [--level LEVEL] [--verbose] [--execute]

Levels:
    - basic: Simple queries (BASIC.1-10)
    - intermediate: Joins and aggregates (INT.1-10)
    - advanced: Complex subqueries (ADV.1-10)
    - production: Production-like queries (PROD.1-5)
    - all: All queries (default)

Options:
    --verbose: Print detailed output
    --execute: Actually run the translated PySpark code (requires Spark)
"""

import re
import ast
import sys
import argparse
import os
from pathlib import Path
from typing import List, Tuple, Dict, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from sql_to_pyspark.translator import translate_sql_to_pyspark


# Global Spark session (initialized lazily)
_spark = None


def get_spark_session():
    """Get or create Spark session."""
    global _spark
    if _spark is None:
        try:
            from pyspark.sql import SparkSession
            _spark = SparkSession.builder \
                .appName("TPC-DS Translator Test") \
                .config("spark.driver.memory", "2g") \
                .config("spark.sql.shuffle.partitions", "4") \
                .getOrCreate()
            _spark.sparkContext.setLogLevel("ERROR")
        except Exception as e:
            error_msg = str(e)
            if "getSubject is not supported" in error_msg:
                print("\n" + "="*80)
                print("ERROR: Java 25 Compatibility Issue")
                print("="*80)
                print("PySpark is not compatible with Java 25.")
                print("\nTo fix this, you have two options:")
                print("\n1. Install Java 11, 17, or 21:")
                print("   brew install openjdk@21")
                print("   export JAVA_HOME=$(/usr/libexec/java_home -v 21)")
                print("\n2. Use validation-only mode (default):")
                print("   python run_tpcds_tests.py --level basic")
                print("   (This validates syntax and PySpark patterns without execution)")
                print("\n" + "="*80 + "\n")
            else:
                print(f"Warning: Could not initialize Spark session: {e}")
                print("Make sure PySpark is installed: pip install pyspark\n")
            return None
    return _spark


def load_csv_data_to_spark(spark, data_dir: Path):
    """Load CSV data into Spark as temporary views."""
    if spark is None:
        return False
    
    try:
        from pyspark.sql.types import (
            StructType, StructField, IntegerType, StringType, DoubleType
        )
        
        # Define schemas for each table
        schemas = {
            'customer': StructType([
                StructField('c_customer_sk', IntegerType(), True),
                StructField('c_customer_id', StringType(), True),
                StructField('c_current_cdemo_sk', IntegerType(), True),
                StructField('c_current_hdemo_sk', IntegerType(), True),
                StructField('c_current_addr_sk', IntegerType(), True),
                StructField('c_first_shipto_date_sk', IntegerType(), True),
                StructField('c_first_sales_date_sk', IntegerType(), True),
                StructField('c_salutation', StringType(), True),
                StructField('c_first_name', StringType(), True),
                StructField('c_last_name', StringType(), True),
                StructField('c_preferred_cust_flag', StringType(), True),
                StructField('c_birth_day', IntegerType(), True),
                StructField('c_birth_month', IntegerType(), True),
                StructField('c_birth_year', IntegerType(), True),
                StructField('c_birth_country', StringType(), True),
                StructField('c_login', StringType(), True),
                StructField('c_email_address', StringType(), True),
                StructField('c_last_review_date', StringType(), True),
            ]),
            'date_dim': StructType([
                StructField('d_date_sk', IntegerType(), True),
                StructField('d_date_id', StringType(), True),
                StructField('d_date', StringType(), True),
                StructField('d_month_seq', IntegerType(), True),
                StructField('d_week_seq', IntegerType(), True),
                StructField('d_quarter_seq', IntegerType(), True),
                StructField('d_year', IntegerType(), True),
                StructField('d_dow', IntegerType(), True),
                StructField('d_moy', IntegerType(), True),
                StructField('d_dom', IntegerType(), True),
                StructField('d_qoy', IntegerType(), True),
                StructField('d_fy_year', IntegerType(), True),
                StructField('d_fy_quarter_seq', IntegerType(), True),
                StructField('d_fy_week_seq', IntegerType(), True),
                StructField('d_day_name', StringType(), True),
                StructField('d_quarter_name', StringType(), True),
                StructField('d_holiday', StringType(), True),
                StructField('d_weekend', StringType(), True),
                StructField('d_following_holiday', StringType(), True),
                StructField('d_first_dom', IntegerType(), True),
                StructField('d_last_dom', IntegerType(), True),
                StructField('d_same_day_ly', IntegerType(), True),
                StructField('d_same_day_lq', IntegerType(), True),
                StructField('d_current_day', StringType(), True),
                StructField('d_current_week', StringType(), True),
                StructField('d_current_month', StringType(), True),
                StructField('d_current_quarter', StringType(), True),
                StructField('d_current_year', StringType(), True),
            ]),
            'item': StructType([
                StructField('i_item_sk', IntegerType(), True),
                StructField('i_item_id', StringType(), True),
                StructField('i_rec_start_date', StringType(), True),
                StructField('i_rec_end_date', StringType(), True),
                StructField('i_item_desc', StringType(), True),
                StructField('i_current_price', DoubleType(), True),
                StructField('i_wholesale_cost', DoubleType(), True),
                StructField('i_brand_id', IntegerType(), True),
                StructField('i_brand', StringType(), True),
                StructField('i_class_id', IntegerType(), True),
                StructField('i_class', StringType(), True),
                StructField('i_category_id', IntegerType(), True),
                StructField('i_category', StringType(), True),
                StructField('i_manufact_id', IntegerType(), True),
                StructField('i_manufact', StringType(), True),
                StructField('i_size', StringType(), True),
                StructField('i_formulation', StringType(), True),
                StructField('i_color', StringType(), True),
                StructField('i_units', StringType(), True),
                StructField('i_container', StringType(), True),
                StructField('i_manager_id', IntegerType(), True),
                StructField('i_product_name', StringType(), True),
            ]),
            'store': StructType([
                StructField('s_store_sk', IntegerType(), True),
                StructField('s_store_id', StringType(), True),
                StructField('s_rec_start_date', StringType(), True),
                StructField('s_rec_end_date', StringType(), True),
                StructField('s_closed_date_sk', IntegerType(), True),
                StructField('s_store_name', StringType(), True),
                StructField('s_number_employees', IntegerType(), True),
                StructField('s_floor_space', IntegerType(), True),
                StructField('s_hours', StringType(), True),
                StructField('s_manager', StringType(), True),
                StructField('s_market_id', IntegerType(), True),
                StructField('s_geography_class', StringType(), True),
                StructField('s_market_desc', StringType(), True),
                StructField('s_market_manager', StringType(), True),
                StructField('s_division_id', IntegerType(), True),
                StructField('s_division_name', StringType(), True),
                StructField('s_company_id', IntegerType(), True),
                StructField('s_company_name', StringType(), True),
                StructField('s_street_number', StringType(), True),
                StructField('s_street_name', StringType(), True),
                StructField('s_street_type', StringType(), True),
                StructField('s_suite_number', StringType(), True),
                StructField('s_city', StringType(), True),
                StructField('s_county', StringType(), True),
                StructField('s_state', StringType(), True),
                StructField('s_zip', StringType(), True),
                StructField('s_country', StringType(), True),
                StructField('s_gmt_offset', DoubleType(), True),
                StructField('s_tax_precentage', DoubleType(), True),
            ]),
            'store_sales': StructType([
                StructField('ss_sold_date_sk', IntegerType(), True),
                StructField('ss_sold_time_sk', IntegerType(), True),
                StructField('ss_item_sk', IntegerType(), True),
                StructField('ss_customer_sk', IntegerType(), True),
                StructField('ss_cdemo_sk', IntegerType(), True),
                StructField('ss_hdemo_sk', IntegerType(), True),
                StructField('ss_addr_sk', IntegerType(), True),
                StructField('ss_store_sk', IntegerType(), True),
                StructField('ss_promo_sk', IntegerType(), True),
                StructField('ss_ticket_number', IntegerType(), True),
                StructField('ss_quantity', IntegerType(), True),
                StructField('ss_wholesale_cost', DoubleType(), True),
                StructField('ss_list_price', DoubleType(), True),
                StructField('ss_sales_price', DoubleType(), True),
                StructField('ss_ext_discount_amt', DoubleType(), True),
                StructField('ss_ext_sales_price', DoubleType(), True),
                StructField('ss_ext_wholesale_cost', DoubleType(), True),
                StructField('ss_ext_list_price', DoubleType(), True),
                StructField('ss_ext_tax', DoubleType(), True),
                StructField('ss_coupon_amt', DoubleType(), True),
                StructField('ss_net_paid', DoubleType(), True),
                StructField('ss_net_paid_inc_tax', DoubleType(), True),
                StructField('ss_net_profit', DoubleType(), True),
            ]),
            'inventory': StructType([
                StructField('inv_date_sk', IntegerType(), True),
                StructField('inv_item_sk', IntegerType(), True),
                StructField('inv_warehouse_sk', IntegerType(), True),
                StructField('inv_quantity_on_hand', IntegerType(), True),
            ])
        }
        
        # Load each CSV file
        for table_name in ['customer', 'date_dim', 'item', 'store', 'store_sales', 'inventory']:
            csv_file = data_dir / f"{table_name}.csv"
            if csv_file.exists():
                df = spark.read.csv(
                    str(csv_file),
                    header=True,
                    schema=schemas.get(table_name)
                )
                df.createOrReplaceTempView(table_name)
                if os.getenv('VERBOSE'):
                    print(f"  Loaded {table_name}: {df.count()} rows")
        
        return True
    except Exception as e:
        print(f"Warning: Could not load CSV data: {e}")
        return False


def execute_pyspark_code(code: str, spark) -> Tuple[bool, Optional[str], Optional[int]]:
    """
    Execute translated PySpark code and return success status.
    
    Returns: (success, error_message, row_count)
    """
    if spark is None:
        return False, "Spark session not available", None
    
    try:
        # Create execution namespace with necessary imports
        namespace = {
            'spark': spark,
        }
        
        # Import common PySpark functions into namespace
        from pyspark.sql import functions as F
        from pyspark.sql import Window
        from pyspark.sql.functions import col, lit, when, sum as spark_sum, count, avg, max as spark_max, min as spark_min, countDistinct
        
        namespace.update({
            'F': F,
            'Window': Window,
            'col': col,
            'lit': lit,
            'when': when,
            'sum': spark_sum,
            'count': count,
            'countDistinct': countDistinct,
            'avg': avg,
            'max': spark_max,
            'min': spark_min,
        })

        # Bind common table views as DataFrame variables for convenience
        for table_name in ['customer', 'date_dim', 'item', 'store', 'store_sales', 'inventory']:
            try:
                namespace[table_name] = spark.table(table_name)
            except Exception:
                # If table doesn't exist, skip; execution will surface real errors
                pass
        
        # Execute the code
        exec(code, namespace)
        
        # Get the result DataFrame (typically assigned to 'result' or 'df')
        result_df = namespace.get('result') or namespace.get('df')
        
        if result_df is not None:
            # Try to collect and count rows (this validates the query actually runs)
            row_count = result_df.count()
            return True, None, row_count
        else:
            return True, None, None
            
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        return False, error_msg, None


def parse_tpcds_queries(file_path: str) -> List[Tuple[str, str, str]]:
    """
    Parse TPC-DS test queries from SQL file.
    
    Returns: List of (query_id, description, query_text) tuples
    """
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    queries = []
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Look for comment line with query ID (e.g., "-- BASIC.1: Simple store sales query")
        if line.startswith('--') and '.' in line and ':' in line:
            # Parse the comment
            parts = line.replace('--', '').strip().split(':', 1)
            query_id = parts[0].strip()
            description = parts[1].strip() if len(parts) > 1 else ""
            
            # Next non-empty line should be the query
            i += 1
            query_lines = []
            while i < len(lines):
                query_line = lines[i].rstrip()
                if not query_line or query_line.startswith('--'):
                    break
                query_lines.append(query_line)
                i += 1
            
            if query_lines:
                query_text = ' '.join(query_lines).strip().rstrip(';')
                queries.append((query_id, description, query_text))
        
        i += 1
    
    return queries





def validate_python_code(code: str) -> Tuple[bool, str]:
    """Validate that generated code is valid Python."""
    try:
        ast.parse(code)
        return True, ""
    except SyntaxError as e:
        return False, f"SyntaxError at line {e.lineno}: {e.msg}"
    except Exception as e:
        return False, f"Error: {str(e)}"


def contains_pyspark_patterns(code: str) -> bool:
    """Check if code contains PySpark-like patterns."""
    pyspark_patterns = [
        r'\.select\(',
        r'\.filter\(',
        r'\.groupBy\(',
        r'\.agg\(',
        r'\.join\(',
        r'\.orderBy\(',
        r'\.limit\(',
        r'\.distinct\(',
        r'col\(',
        r'spark\.',
        r'df\.',
    ]
    return any(re.search(pattern, code) for pattern in pyspark_patterns)


def filter_queries_by_level(queries: List[Tuple[str, str, str]], level: str) -> List[Tuple[str, str, str]]:
    """Filter queries by complexity level."""
    if level == 'all':
        return queries
    
    filtered = []
    level_prefix = level.upper()
    
    for query in queries:
        if query[0].startswith(level_prefix):
            filtered.append(query)
    
    return filtered


def test_translator(queries: List[Tuple[str, str, str]], verbose: bool = False, execute: bool = False) -> Dict[str, any]:
    """Test the translator on all queries."""
    results = {
        'total': len(queries),
        'successful': 0,
        'failed': 0,
        'by_level': {},
        'details': []
    }
    
    spark = None
    data_loaded = False
    
    # Initialize Spark session and load data if execution is requested
    if execute:
        spark = get_spark_session()
        if spark is None:
            print("⚠️  Spark not available - falling back to validation-only mode\n")
            execute = False  # Disable execution, continue with validation
        else:
            # Load CSV data into Spark
            script_dir = Path(__file__).parent
            data_dir = script_dir / "tpcds_data"
            if not data_dir.exists():
                print(f"Warning: Data directory {data_dir} not found. Execution tests will fail.")
            else:
                print(f"Loading CSV data from {data_dir}...")
                data_loaded = load_csv_data_to_spark(spark, data_dir)
                if data_loaded:
                    print("✓ Data loaded successfully!\n")
                else:
                    print("Warning: Failed to load data. Execution tests may fail.\n")
    
    for query_id, description, query_text in queries:
        level = query_id.split('.')[0]
        
        if level not in results['by_level']:
            results['by_level'][level] = {'pass': 0, 'fail': 0}
        
        try:
            # Translate query
            pyspark_code = translate_sql_to_pyspark(query_text)
            
            # Validate Python syntax
            is_valid, error_msg = validate_python_code(pyspark_code)
            
            if not is_valid:
                results['failed'] += 1
                results['by_level'][level]['fail'] += 1
                results['details'].append({
                    'id': query_id,
                    'description': description,
                    'status': 'SYNTAX_ERROR',
                    'error': error_msg,
                    'code': pyspark_code[:150] + "..." if len(pyspark_code) > 150 else pyspark_code
                })
                if verbose:
                    print(f"✗ {query_id}: {error_msg}")
                continue
            
            # Check for PySpark patterns
            has_pyspark = contains_pyspark_patterns(pyspark_code)
            
            if not has_pyspark:
                results['failed'] += 1
                results['by_level'][level]['fail'] += 1
                results['details'].append({
                    'id': query_id,
                    'description': description,
                    'status': 'NO_PYSPARK',
                    'error': 'No PySpark patterns detected',
                    'code': pyspark_code[:150] + "..." if len(pyspark_code) > 150 else pyspark_code
                })
                if verbose:
                    print(f"⚠ {query_id}: No PySpark patterns (fallback)")
                continue
            
            # Execute the code if --execute flag is set
            execution_success = True
            row_count = None
            if execute:
                execution_success, exec_error, row_count = execute_pyspark_code(pyspark_code, spark)
                
                if not execution_success:
                    results['failed'] += 1
                    results['by_level'][level]['fail'] += 1
                    results['details'].append({
                        'id': query_id,
                        'description': description,
                        'status': 'EXECUTION_ERROR',
                        'error': exec_error,
                        'code': pyspark_code[:150] + "..." if len(pyspark_code) > 150 else pyspark_code
                    })
                    if verbose:
                        print(f"✗ {query_id}: Execution error - {exec_error}")
                    continue
            
            # Success
            results['successful'] += 1
            results['by_level'][level]['pass'] += 1
            results['details'].append({
                'id': query_id,
                'description': description,
                'status': 'PASS',
                'code_length': len(pyspark_code),
                'row_count': row_count
            })
            if verbose:
                print(f"✓ {query_id}: {description} ({row_count} rows)")
            
        except Exception as e:
            results['failed'] += 1
            results['by_level'][level]['fail'] += 1
            results['details'].append({
                'id': query_id,
                'description': description,
                'status': 'ERROR',
                'error': str(e)
            })
            if verbose:
                print(f"✗ {query_id}: {str(e)}")
    
    return results


def print_results(results: Dict[str, any], verbose: bool = False):
    """Print test results in a formatted way."""
    print("\n" + "="*80)
    print("TPC-DS SQL-TO-PYSPARK TRANSLATOR TEST RESULTS")
    print("="*80)
    
    # Summary
    print(f"\nTotal Queries:    {results['total']}")
    print(f"Successful:       {results['successful']} ({100*results['successful']/max(1, results['total']):.1f}%)")
    print(f"Failed:           {results['failed']} ({100*results['failed']/max(1, results['total']):.1f}%)")
    
    # By level
    if results['by_level']:
        print("\n" + "-"*80)
        print("RESULTS BY LEVEL:")
        print("-"*80)
        
        for level in sorted(results['by_level'].keys()):
            stats = results['by_level'][level]
            total = stats['pass'] + stats['fail']
            pct = 100 * stats['pass'] / max(1, total)
            status = "✓" if pct == 100 else "⚠" if pct >= 80 else "✗"
            print(f"{status} {level:12s}: {stats['pass']:2d}/{total:2d} passed ({pct:5.1f}%)")
    
    # Failed details
    failed_details = [d for d in results['details'] if d['status'] != 'PASS']
    if failed_details:
        print("\n" + "-"*80)
        print("FAILED/WARNING QUERIES:")
        print("-"*80)
        
        for detail in failed_details:
            print(f"\n{detail['id']}: {detail['description']}")
            print(f"  Status: {detail['status']}")
            if 'error' in detail:
                print(f"  Error: {detail['error']}")
            if 'code' in detail:
                print(f"  Code: {detail['code']}")
    
    # Success details (if verbose or execution mode)
    if verbose:
        success_details = [d for d in results['details'] if d['status'] == 'PASS']
        if success_details:
            print("\n" + "-"*80)
            print("SUCCESSFUL QUERIES:")
            print("-"*80)
            
            for detail in success_details:
                row_info = f" - {detail['row_count']} rows" if detail.get('row_count') is not None else ""
                print(f"✓ {detail['id']}: {detail['description']}{row_info}")
    
    print("\n" + "="*80 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Test SQL-to-PySpark translator with TPC-DS queries")
    parser.add_argument("--level", choices=['basic', 'intermediate', 'advanced', 'production', 'all'], 
                       default='all', help="Filter by complexity level")
    parser.add_argument("--verbose", "-v", action='store_true', help="Print detailed output")
    parser.add_argument("--execute", action='store_true', help="Execute translated PySpark code")
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    queries_file = script_dir / "tpcds_test_queries.sql"
    
    if not queries_file.exists():
        print(f"Error: {queries_file} not found")
        sys.exit(1)
    
    print("Parsing TPC-DS test queries...")
    queries = parse_tpcds_queries(str(queries_file))
    
    # Filter by level
    filtered_queries = filter_queries_by_level(queries, args.level)
    print(f"Found {len(filtered_queries)} queries at level '{args.level}'\n")
    
    if args.execute:
        print("Mode: EXECUTION (will actually run translated PySpark code)")
    else:
        print("Mode: VALIDATION (syntax and pattern checking only)")
        print("Use --execute flag to actually run the code\n")
    
    print("Running translator tests...")
    results = test_translator(filtered_queries, verbose=args.verbose, execute=args.execute)
    
    print_results(results, verbose=args.verbose)
    
    # Save results
    output_file = script_dir / "tpcds_test_results.txt"
    with open(output_file, 'w') as f:
        f.write("TPC-DS SQL-TO-PYSPARK TRANSLATOR TEST RESULTS\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"Total Queries: {results['total']}\n")
        f.write(f"Successful: {results['successful']}\n")
        f.write(f"Failed: {results['failed']}\n\n")
        
        f.write("RESULTS BY LEVEL:\n")
        for level in sorted(results['by_level'].keys()):
            stats = results['by_level'][level]
            total = stats['pass'] + stats['fail']
            f.write(f"{level}: {stats['pass']}/{total}\n")
        
        f.write("\n\nDETAILED RESULTS:\n")
        for detail in results['details']:
            f.write(f"\n{detail['id']}: {detail['description']}\n")
            f.write(f"Status: {detail['status']}\n")
            if 'error' in detail:
                f.write(f"Error: {detail['error']}\n")
    
    print(f"Results saved to: {output_file}\n")
    
    sys.exit(0 if results['failed'] == 0 else 1)


if __name__ == "__main__":
    main()
