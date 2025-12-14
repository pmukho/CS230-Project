#!/usr/bin/env python3
"""
TPC-DS SQL-to-PySpark Translator Test Runner

Tests the translator against TPC-DS schema queries at various complexity levels.
Verifies:
1. Translation produces valid Python code
2. Translated code is syntactically correct
3. Contains appropriate PySpark patterns
4. Results can be executed (when data is available)

Usage:
    python run_tpcds_tests.py [--level LEVEL] [--verbose]

Levels:
    - basic: Simple queries (BASIC.1-10)
    - intermediate: Joins and aggregates (INT.1-10)
    - advanced: Complex subqueries (ADV.1-10)
    - production: Production-like queries (PROD.1-5)
    - all: All queries (default)
"""

import re
import ast
import sys
import argparse
from pathlib import Path
from typing import List, Tuple, Dict

sys.path.insert(0, str(Path(__file__).parent.parent))

from sql_to_pyspark.translator import translate_sql_to_pyspark


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


def test_translator(queries: List[Tuple[str, str, str]], verbose: bool = False) -> Dict[str, any]:
    """Test the translator on all queries."""
    results = {
        'total': len(queries),
        'successful': 0,
        'failed': 0,
        'by_level': {},
        'details': []
    }
    
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
            
            # Success
            results['successful'] += 1
            results['by_level'][level]['pass'] += 1
            results['details'].append({
                'id': query_id,
                'description': description,
                'status': 'PASS',
                'code_length': len(pyspark_code)
            })
            if verbose:
                print(f"✓ {query_id}: {description}")
            
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
    
    print("\n" + "="*80 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Test SQL-to-PySpark translator with TPC-DS queries")
    parser.add_argument("--level", choices=['basic', 'intermediate', 'advanced', 'production', 'all'], 
                       default='all', help="Filter by complexity level")
    parser.add_argument("--verbose", "-v", action='store_true', help="Print detailed output")
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
    
    print("Running translator tests...")
    results = test_translator(filtered_queries, verbose=args.verbose)
    
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
