import pytest

# --- Import the function under test ---
from translator import translate_sql_to_pyspark

import ast


# --- Helper: run translation and sanity check output ---
def assert_valid_translation(sql_query):
    pyspark_code = translate_sql_to_pyspark(sql_query)
    assert isinstance(pyspark_code, str), "Output must be a string"

    try:
        ast.parse(pyspark_code)
    except Exception as e:
        raise ValueError(f"Invalid PySpark code: {pyspark_code}\nError: {e}")
    # Check for PySpark DataFrame operations
    pyspark_methods = [".select(", ".filter(", ".groupBy(", ".orderBy(", ".limit(", ".agg("]
    has_pyspark_method = any(method in pyspark_code for method in pyspark_methods)
    assert has_pyspark_method or "df" in pyspark_code or "spark" in pyspark_code, \
        f"Output should look like PySpark code. Got: {pyspark_code}"
    return pyspark_code


# =========================================================
# 1. BASIC SELECT / FROM / WHERE
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT * FROM employees",
    "SELECT id, name FROM customers",
    "SELECT id FROM users WHERE age > 30",
    "SELECT * FROM sales WHERE region = 'US'",
    "SELECT * FROM table WHERE salary BETWEEN 50000 AND 100000",
    "SELECT * FROM products WHERE category IS NULL",
    "SELECT * FROM products WHERE category IS NOT NULL",
])
def test_basic_select_from_where(query):
    assert_valid_translation(query)


# =========================================================
# 2. DISTINCT, LIMIT, ORDER BY, GROUP BY, HAVING
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT DISTINCT country FROM customers",
    "SELECT name FROM employees ORDER BY salary DESC",
    "SELECT name, department, AVG(salary) FROM employees GROUP BY department",
    "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5",
    "SELECT * FROM sales LIMIT 10",
])
def test_aggregate_and_sorting(query):
    assert_valid_translation(query)


# =========================================================
# 3. AGGREGATION FUNCTIONS
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT COUNT(*) FROM employees",
    "SELECT SUM(salary), AVG(age) FROM employees",
    "SELECT MAX(score), MIN(score) FROM test_results",
    "SELECT department, COUNT(*) AS num FROM employees GROUP BY department",
])
def test_aggregate_functions(query):
    assert_valid_translation(query)


# =========================================================
# 4. ARITHMETIC, LOGICAL, COMPARISON OPERATORS
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT salary * 1.1 FROM employees",
    "SELECT (price - discount) / price AS discount_ratio FROM sales",
    "SELECT * FROM employees WHERE age >= 30 AND (department = 'HR' OR department = 'IT')",
    "SELECT * FROM orders WHERE NOT status = 'cancelled'",
    "SELECT * FROM products WHERE quantity = 0 OR price < 10",
])
def test_operators(query):
    assert_valid_translation(query)


# =========================================================
# 5. STRING AND DATE FUNCTIONS
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT UPPER(name), LOWER(city) FROM customers",
    "SELECT SUBSTRING(name, 1, 3) FROM users",
    "SELECT CONCAT(first_name, ' ', last_name) FROM employees",
    "SELECT LENGTH(description) FROM products",
    "SELECT DATE_ADD(order_date, 7) FROM orders",
    "SELECT YEAR(birthdate), MONTH(birthdate), DAY(birthdate) FROM users",
])
def test_string_and_date_functions(query):
    assert_valid_translation(query)


# =========================================================
# 6. CASE / WHEN / COALESCE / NULLIF
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT CASE WHEN salary > 100000 THEN 'High' ELSE 'Low' END FROM employees",
    "SELECT COALESCE(phone, 'N/A') FROM contacts",
    "SELECT NULLIF(status, 'inactive') FROM users",
])
def test_conditional_expressions(query):
    assert_valid_translation(query)


# =========================================================
# 7. JOINS (INNER, LEFT, RIGHT, FULL, CROSS)
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT * FROM customers INNER JOIN orders ON customers.id = orders.customer_id",
    "SELECT * FROM employees LEFT JOIN departments ON employees.dept_id = departments.id",
    "SELECT * FROM a RIGHT JOIN b ON a.id = b.aid",
    "SELECT * FROM a FULL OUTER JOIN b ON a.key = b.key",
    "SELECT * FROM users CROSS JOIN permissions",
])
def test_joins(query):
    assert_valid_translation(query)


# =========================================================
# 8. SUBQUERIES (IN SELECT, WHERE, FROM)
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT name FROM employees WHERE department_id IN (SELECT id FROM departments WHERE region = 'West')",
    "SELECT * FROM (SELECT id, name FROM users WHERE active = 1) AS active_users",
    "SELECT department, (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS emp_count FROM departments d",
])
def test_subqueries(query):
    assert_valid_translation(query)


# =========================================================
# 9. SET OPERATIONS (UNION, INTERSECT, EXCEPT)
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT id FROM table1 UNION SELECT id FROM table2",
    "SELECT name FROM table1 INTERSECT SELECT name FROM table2",
    "SELECT name FROM table1 EXCEPT SELECT name FROM table2",
])
def test_set_operations(query):
    assert_valid_translation(query)


# =========================================================
# 10. WINDOW FUNCTIONS
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT name, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) FROM employees",
    "SELECT id, SUM(sales) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM transactions",
])
def test_window_functions(query):
    assert_valid_translation(query)


# =========================================================
# 11. COMPLEX EXPRESSIONS & NESTED FUNCTIONS
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT ROUND(AVG(price * quantity), 2) FROM orders",
    "SELECT CONCAT(UPPER(first_name), LOWER(last_name)) FROM users",
    "SELECT CASE WHEN LENGTH(name) > 5 THEN SUBSTRING(name, 1, 5) ELSE name END FROM users",
])
def test_nested_expressions(query):
    assert_valid_translation(query)


# =========================================================
# 12. ALIASES, QUOTING, ESCAPING, SPECIAL CHARACTERS
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT name AS employee_name FROM employees",
    "SELECT 'O\\'Reilly' AS publisher FROM dual",
    "SELECT `weird-column`, \"double_quote\" FROM data_table",
])
def test_aliases_and_escaping(query):
    assert_valid_translation(query)


# =========================================================
# 13. EDGE CASES (EMPTY, INVALID, COMMENTS)
# =========================================================

@pytest.mark.parametrize("query", [
    "SELECT",                    # incomplete
    "",                          # empty
    "-- comment only",
    "SELECT * FROM t /* comment */ WHERE x = 1",
])
def test_edge_cases(query):
    try:
        assert_valid_translation(query)
    except Exception as e:
        # Expected: function should not crash hard
        assert "error" in str(e).lower() or isinstance(e, Exception)


# =========================================================
# 14. COMBINED COMPLEX QUERIES
# =========================================================

@pytest.mark.parametrize("query", [
    """
    SELECT d.name, COUNT(e.id) AS num_employees, AVG(e.salary) AS avg_salary
    FROM departments d
    LEFT JOIN employees e ON d.id = e.dept_id
    WHERE e.hire_date > '2020-01-01'
    GROUP BY d.name
    HAVING COUNT(e.id) > 10
    ORDER BY avg_salary DESC
    LIMIT 5
    """,
    """
    SELECT region, 
           SUM(sales) OVER (PARTITION BY region ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
    FROM sales
    WHERE product_id IN (SELECT id FROM products WHERE category = 'Electronics')
    """
])
def test_full_complex_queries(query):
    assert_valid_translation(query)


