# copied from https://sqlandhadoop.com/online-sql-to-pyspark-converter/

from mo_sql_parsing import parse
from mo_sql_parsing import format
import json

def translate_sql_to_pyspark(query: str) -> str:

    v_parse = parse(query)
    v_json = json.loads(json.dumps(v_parse,indent=4))

    # # # print(f'v_json: {v_json}')
    
    # Define aggregate functions (functions that combine multiple rows)
    AGGREGATE_FUNCTIONS = {
        'count', 'sum', 'avg', 'mean', 'max', 'min',
        'stddev', 'stddev_samp', 'stddev_pop',
        'variance', 'var_samp', 'var_pop',
        'collect_list', 'collect_set',
        'first', 'last', 'approx_count_distinct'
    }

    SPECIAL_FUNCTION_MAPPING = {
        "date_add": "dateadd",
        "str_to_date": "to_date",
        "substr": "substring",
    }
    
    def is_aggregate_function(value_dict):
        """Check if a value dict contains an aggregate function"""
        if type(value_dict) is not dict:
            return False
        for key in value_dict.keys():
            if key.lower() in AGGREGATE_FUNCTIONS:
                return True
        return False

    def is_window_expression(select_item):
        """Detect whether a select item is a window expression (has OVER)."""
        if type(select_item) is not dict:
            return False
        if 'over' in select_item:
            # # # print(f'{select_item} is a window function')
            return True
        # Look inside value
        val = select_item.get('value')
        if type(val) is dict:
            if 'over' in val:
                # # # print(f'{select_item} is a window function (value)')
                return True
            # function form: {'sum': {'over': {...}, 'value': 'col'}}
            try:
                inner_key = list(val.keys())[0]
                inner_val = val[inner_key]
                if type(inner_val) is dict and 'over' in inner_val:
                    # # # print(f'{select_item} is a window function (inner_val)')
                    return True
            except Exception:
                pass
        return False
    
    def calculate_nesting_depth(query_dict, current_depth=0):
        """Calculate the nesting depth of subqueries in a query structure"""
        if type(query_dict) is not dict:
            return current_depth
        
        max_depth = current_depth
        
        # Check if this is a SELECT query (subquery)
        if 'select' in query_dict or 'select_distinct' in query_dict:
            current_depth += 1
            max_depth = max(max_depth, current_depth)
        
        # Recursively check all values
        for key, value in query_dict.items():
            if type(value) is dict:
                depth = calculate_nesting_depth(value, current_depth)
                max_depth = max(max_depth, depth)
            elif type(value) is list:
                for item in value:
                    if type(item) is dict:
                        depth = calculate_nesting_depth(item, current_depth)
                        max_depth = max(max_depth, depth)
        
        return max_depth
    
    def extract_correlation_info(subquery_dict, outer_alias=None):
        """Extract correlation information from a scalar subquery's WHERE clause"""
        if 'where' not in subquery_dict:
            return None
        
        where_clause = subquery_dict['where']
        correlations = []
        
        def find_correlations(clause):
            if type(clause) is not dict:
                return
            
            # Look for equality conditions that might be correlations
            if 'eq' in clause:
                operands = clause['eq']
                if type(operands) is list and len(operands) == 2:
                    left, right = operands[0], operands[1]
                    # Check if one side references outer table
                    if type(left) is str and type(right) is str:
                        if '.' in left and '.' in right:
                            # Both are qualified - potential correlation
                            correlations.append({
                                'left': left,
                                'right': right,
                                'operator': '=='
                            })
            
            # Recursively check AND/OR conditions
            for key in ['and', 'or']:
                if key in clause:
                    conditions = clause[key]
                    if type(conditions) is list:
                        for cond in conditions:
                            find_correlations(cond)
        
        find_correlations(where_clause)
        return correlations if correlations else None
    
    def collect_aggregates(expr):
        """Recursively collect all aggregate functions from an expression."""
        aggs = []
        if type(expr) is dict:
            if is_aggregate_function(expr) and not is_window_expression(expr):
                return [expr]
            for k, v in expr.items():
                if k != 'literal': # skip literals
                    aggs.extend(collect_aggregates(v))
        elif type(expr) is list:
            for item in expr:
                aggs.extend(collect_aggregates(item))
        return aggs

    def replace_aggregates_with_aliases(expr, agg_aliases):
        """Recursively replace aggregate functions with their aliases."""
        # # print(f"DEBUG: Replacing aggregates with aliases: {expr}")
        # # print(f"DEBUG: Agg aliases: {agg_aliases}")
        if type(expr) is dict:
            if is_aggregate_function(expr) and not is_window_expression(expr):
                # Try to find matching alias
                agg_str = get_agg_key(expr)
                if agg_aliases and agg_str in agg_aliases:
                    return agg_aliases[agg_str]
            # Recurse
            return {k: replace_aggregates_with_aliases(v, agg_aliases) for k, v in expr.items()}
        elif type(expr) is list:
            return [replace_aggregates_with_aliases(item, agg_aliases) for item in expr]
        return expr

    def extract_aggregate_from_expr(expr):
        """
        Traverse an expression dict to find the aggregate function.
        Returns (agg_func, agg_col, template_expr)
        template_expr is the original expr with the aggregate replaced by a placeholder string.
        """
        if type(expr) is dict:
            # Check if this node IS the aggregate
            if is_aggregate_function(expr) and not is_window_expression(expr):
                func_name = list(expr.keys())[0]
                col_val = expr[func_name]
                return func_name.lower(), col_val, "__AGG_PLACEHOLDER__"
            
            # Recursive check
            for k, v in expr.items():
                if k == 'literal': continue
                res = extract_aggregate_from_expr(v)
                if res:
                    agg_func, agg_col, sub_template = res
                    # Return new dict with this value replaced
                    new_expr = expr.copy()
                    new_expr[k] = sub_template
                    return agg_func, agg_col, new_expr
            return None
                    
        elif type(expr) is list:
            for i, item in enumerate(expr):
                res = extract_aggregate_from_expr(item)
                if res:
                    agg_func, agg_col, sub_template = res
                    # Reconstruct list
                    new_list = list(expr)
                    new_list[i] = sub_template
                    return agg_func, agg_col, new_list
            return None
        
        return None
    
    def translate_scalar_subquery_join(subquery_dict, alias_name, outer_table_info):
        """
        Translate a scalar subquery to PySpark using JOIN method.
        Returns a dict with aggregation DataFrame code and join information.
        """
        # # # print(f'DEBUG: translating subquery: {subquery_dict}')
        # Extract the aggregate function from SELECT
        select_data = subquery_dict.get('select') or subquery_dict.get('select_distinct')
        if not select_data:
            return None
        
        # Get the aggregate function and potential wrapper expression
        agg_func = None
        agg_column = None
        template_expr = None
        
        if type(select_data) is dict and 'value' in select_data:
            value = select_data['value']
            # Use helper to extract aggregate from complex expressions (e.g., avg(x) * 1.2)
            res = extract_aggregate_from_expr(value)
            if res:
                agg_func, agg_column, template_expr = res
        
        if not agg_func:
            # Fallback for non-aggregate scalar subqueries (e.g. SELECT distinct col)
            # OR for EXISTS subqueries (SELECT *)
            # Treat as first() which is appropriate for scalar single-row results
            if type(select_data) is dict:
                 val = select_data.get('value')
                 if type(val) is str:
                     agg_func = 'first'
                     agg_column = val
                     template_expr = "__AGG_PLACEHOLDER__"
                 elif 'all_columns' in select_data:
                     # Handle SELECT *
                     agg_func = 'first'
                     agg_column = '*'
                     template_expr = "__AGG_PLACEHOLDER__"
        
        if not agg_func:
            return None
        
        # Helper to rebuild expression string from template
        def rebuild_expr(tmpl, inner_agg_str):
            if tmpl == "__AGG_PLACEHOLDER__":
                return inner_agg_str
            if type(tmpl) is dict:
                # Check for arithmetic in template
                arithmetic_map = {'add': '+', 'sub': '-', 'mul': '*', 'div': '/', 'mod': '%'}
                
                # Use translate_function logic but recursing manually to catch placeholder
                func_name = list(tmpl.keys())[0]
                args = tmpl[func_name]
                
                if type(args) is list:
                    arg_strs = [rebuild_expr(a, inner_agg_str) for a in args]
                    if func_name in arithmetic_map and len(arg_strs) == 2:
                        return f"({arg_strs[0]} {arithmetic_map[func_name]} {arg_strs[1]})"
                    pyspark_func = SPECIAL_FUNCTION_MAPPING.get(func_name, func_name.lower())
                    return f'{pyspark_func}({", ".join(arg_strs)})'
                else:
                    arg_str = rebuild_expr(args, inner_agg_str)
                    pyspark_func = SPECIAL_FUNCTION_MAPPING.get(func_name, func_name.lower())
                    return f'{pyspark_func}({arg_str})'
            elif type(tmpl) in (int, float):
                return str(tmpl)
            else:
                return str(tmpl)
        
        # Store the template for later use in generating the agg expression
        # The join builder will need to know if it's a simple agg or complex
        
        # Store the template for later use in generating the agg expression
        # The join builder will need to know if it's a simple agg or complex
        if agg_column == "*":
            base_agg = f'{agg_func}(lit(1))'
        else:
            base_agg = f'{agg_func}(col("{agg_column}"))'
            
        full_agg_expr = rebuild_expr(template_expr, base_agg)
        
        # Get the FROM table
        from_table = subquery_dict.get('from')
        inner_table = None
        inner_alias = 'subq'
        
        if from_table:
            # Use fn_from to handle string, dict, or list (implicit joins)
            inner_table_str = fn_from(from_table)
            # Wrap in parens to treat as a table expression
            inner_table = f"({inner_table_str})"
            if type(from_table) is dict and 'name' in from_table:
                inner_alias = from_table['name']
        else:
            return None
        
        # Get WHERE clause to find correlation
        where_clause = subquery_dict.get('where')
        correlation_column = None
        other_filters = []
        outer_column = None
        
        # Helper to check if a condition is a correlation
        def check_correlation(left, right):
            # Check if one side looks like an outer reference
            left_str = str(left)
            right_str = str(right)
            
            is_left_outer = False
            is_right_outer = False
            
            # Check if using passed outer aliases
            if type(outer_table_info) is list:
                for alias in outer_table_info:
                    if left_str.startswith(f"{alias}."):
                        is_left_outer = True
                        break
                    if right_str.startswith(f"{alias}."):
                        is_right_outer = True
                        break
            elif type(outer_table_info) is str:
                # Single alias
                if left_str.startswith(f"{outer_table_info}."):
                    is_left_outer = True
                if right_str.startswith(f"{outer_table_info}."):
                    is_right_outer = True
            
            if is_left_outer and not is_right_outer:
                # Left is outer, Right is inner
                return left_str, right_str # (outer, inner)
            elif is_right_outer and not is_left_outer:
                # Right is outer, Left is inner
                return right_str, left_str # (outer, inner)
            elif '.' in left_str and '.' in right_str:
                # Fallback: both qualified
                # Check if one matches inner_alias (if we know it)
                left_parts = left_str.split('.')
                right_parts = right_str.split('.')
                
                # If we detected inner_alias from from clause
                if left_parts[0] == inner_alias:
                    return right_str, left_parts[1] if len(left_parts) > 1 else left_str
                if right_parts[0] == inner_alias:
                    return left_str, right_parts[1] if len(right_parts) > 1 else right_str
            
            return None, None

        if where_clause and type(where_clause) is dict:
            # Check for simple equality correlation
            if 'eq' in where_clause:
                operands = where_clause['eq']
                if type(operands) is list and len(operands) == 2:
                    o_col, i_col = check_correlation(operands[0], operands[1])
                    if o_col and i_col:
                        outer_column = o_col
                        correlation_column = i_col
                    else:
                        other_filters.append(where_clause)
            
            # Handle AND conditions with multiple filters
            elif 'and' in where_clause:
                conditions = where_clause['and']
                if type(conditions) is list:
                    for cond in conditions:
                        if type(cond) is dict and 'eq' in cond:
                            operands = cond['eq']
                            if type(operands) is list and len(operands) == 2:
                                o_col, i_col = check_correlation(operands[0], operands[1])
                                if o_col and i_col:
                                    # Found correlation
                                    outer_column = o_col
                                    correlation_column = i_col
                                else:
                                    other_filters.append(cond)
                        else:
                            other_filters.append(cond)
            else:
                other_filters.append(where_clause)
        
        # Build the aggregation code
        result = {
            'inner_table': inner_table,
            'inner_alias': inner_alias,
            'agg_func': agg_func,
            'agg_column': agg_column,
            'full_agg_expr': full_agg_expr,
            'correlation_column': correlation_column,
            'outer_column': outer_column if correlation_column else None,
            'alias_name': alias_name,
            'other_filters': other_filters
        }
        
        return result
    
    def translate_condition(cond_dict):
        """Translate a condition (like 'gt', 'lt', 'eq') to PySpark syntax"""
        # # # print(f'translating condition: {cond_dict}')
        if type(cond_dict) is not dict:
            return str(cond_dict)
        
        # Handle AND conditions
        if 'and' in cond_dict:
            conditions = cond_dict['and']
            if type(conditions) is list:
                translated_conditions = []
                for cond in conditions:
                    translated = translate_condition(cond)
                    if translated:
                        translated_conditions.append(f"({translated})")
                if translated_conditions:
                    return " & ".join(translated_conditions)
        
        # Handle OR conditions
        if 'or' in cond_dict:
            conditions = cond_dict['or']
            if type(conditions) is list:
                translated_conditions = []
                for cond in conditions:
                    translated = translate_condition(cond)
                    if translated:
                        translated_conditions.append(f"({translated})")
                if translated_conditions:
                    return " | ".join(translated_conditions)
        
        # Helper to format literal values for APIs like between/isin/like (no col()/lit())
        def to_python_literal(val):
            if type(val) is dict:
                if 'literal' in val:
                    lit_val = val['literal']
                    if type(lit_val) is str:
                        return f"'" + lit_val.replace("'", "\\'") + "'"
                    else:
                        return str(lit_val)
                # Handle complex expressions (e.g. arithmetic, functions)
                return translate_value(val)
            elif type(val) is str:
                return f"'" + val.replace("'", "\\'") + "'"
            elif type(val) in (int, float):
                return str(val)
            else:
                return str(val)
        
        # BETWEEN / NOT BETWEEN
        if 'between' in cond_dict:
            between_val = cond_dict['between']
            # Support formats: {'between': [col, low, high]} or {'between': {'expr': col, 'low': x, 'high': y}}
            if type(between_val) is list and len(between_val) == 3:
                col_expr, low, high = between_val
            elif type(between_val) is dict and all(k in between_val for k in ['expr','low','high']):
                col_expr, low, high = between_val['expr'], between_val['low'], between_val['high']
            else:
                col_expr, low, high = None, None, None
            if col_expr is not None:
                left_str = f'col("{col_expr}")' if type(col_expr) is str else str(col_expr)
                return f"{left_str}.between({to_python_literal(low)}, {to_python_literal(high)})"
        if 'not between' in cond_dict:
            nb_val = cond_dict['not between']
            if type(nb_val) is list and len(nb_val) == 3:
                col_expr, low, high = nb_val
            elif type(nb_val) is dict and all(k in nb_val for k in ['expr','low','high']):
                col_expr, low, high = nb_val['expr'], nb_val['low'], nb_val['high']
            else:
                col_expr, low, high = None, None, None
            if col_expr is not None:
                left_str = f'col("{col_expr}")' if type(col_expr) is str else str(col_expr)
                return f"~({left_str}.between({to_python_literal(low)}, {to_python_literal(high)}))"
        
        # IS NULL / IS NOT NULL
        if 'is' in cond_dict:
            operands = cond_dict['is']
            if (type(operands) is list and len(operands) == 2 and operands[1] is None) or (type(operands) is dict and operands.get('right') is None):
                left = operands[0] if type(operands) is list else operands.get('left')
                left_str = f'col("{left}")' if type(left) is str else str(left)
                return f"{left_str}.isNull()"
        if 'is not' in cond_dict:
            operands = cond_dict['is not']
            if (type(operands) is list and len(operands) == 2 and operands[1] is None) or (type(operands) is dict and operands.get('right') is None):
                left = operands[0] if type(operands) is list else operands.get('left')
                left_str = f'col("{left}")' if type(left) is str else str(left)
                return f"{left_str}.isNotNull()"
        # Alternative mo-sql-parsing forms for null checks
        if 'missing' in cond_dict:
            field = cond_dict['missing']
            if field is not None:
                left_str = f'col("{field}")' if type(field) is str else str(field)
                return f"{left_str}.isNull()"
        if 'exists' in cond_dict:
            field = cond_dict['exists']
            if field is not None:
                left_str = f'col("{field}")' if type(field) is str else str(field)
                return f"{left_str}.isNotNull()"
        
        # IN / NOT IN with list (not subquery)
        if 'in' in cond_dict:
            in_val = cond_dict['in']
            if type(in_val) is list and len(in_val) == 2:
                column = in_val[0]
                values_container = in_val[1]
                
                values = []
                if type(values_container) is list:
                    values = values_container
                elif type(values_container) is dict and 'literal' in values_container:
                    lit_val = values_container['literal']
                    if type(lit_val) is list:
                        values = lit_val
                    else:
                        values = [lit_val]
                
                # Only proceed if we found a list of values (not a subquery)
                # Subqueries are dicts with 'select' usually, which fall through here
                if values:
                    # Handle complex expression on LHS
                    if type(column) is dict:
                        left_str = translate_value(column)
                    else:
                        left_str = f'col("{column}")' if type(column) is str else str(column)
                    
                    items = ", ".join(to_python_literal(v) for v in values)
                    return f"{left_str}.isin([{items}])"

        if 'nin' in cond_dict or 'not in' in cond_dict:
            nin_val = cond_dict.get('nin', cond_dict.get('not in'))
            if type(nin_val) is list and len(nin_val) == 2:
                column = nin_val[0]
                values_container = nin_val[1]
                
                values = []
                if type(values_container) is list:
                    values = values_container
                elif type(values_container) is dict and 'literal' in values_container:
                    lit_val = values_container['literal']
                    if type(lit_val) is list:
                        values = lit_val
                    else:
                        values = [lit_val]
                
                if values:
                    # Handle complex expression on LHS
                    if type(column) is dict:
                        left_str = translate_value(column)
                    else:
                        left_str = f'col("{column}")' if type(column) is str else str(column)
                    
                    items = ", ".join(to_python_literal(v) for v in values)
                    return f"~({left_str}.isin([{items}]))"
        
        # LIKE / NOT LIKE -> startswith/endswith/contains when possible
        def translate_like(col_expr, pattern):
            # # # print(f'translating like: {col_expr}, {pattern}')
            left_str = f'col("{col_expr}")' if type(col_expr) is str else str(col_expr)
            if type(pattern) is dict and 'literal' in pattern:
                pat = str(pattern['literal'])
            else:
                pat = str(pattern)
            if pat.startswith('%') and pat.endswith('%') and len(pat) >= 2:
                inner = pat[1:-1].replace("'", "\\'")
                return f"{left_str}.contains('{inner}')"
            if pat.endswith('%') and not pat.startswith('%'):
                prefix = pat[:-1].replace("'", "\\'")
                return f"{left_str}.startswith('{prefix}')"
            if pat.startswith('%') and not pat.endswith('%'):
                suffix = pat[1:].replace("'", "\\'")
                return f"{left_str}.endswith('{suffix}')"
            # Fallback to like
            pat_escaped = pat.replace("'", "\\'")
            return f"{left_str}.like('{pat_escaped}')"
        if 'like' in cond_dict:
            operands = cond_dict['like']
            if type(operands) is list and len(operands) == 2:
                return translate_like(operands[0], operands[1])
        if 'not like' in cond_dict or 'nlike' in cond_dict:
            operands = cond_dict.get('not like', cond_dict.get('nlike'))
            if type(operands) is list and len(operands) == 2:
                return f"~({translate_like(operands[0], operands[1])})"
        
        # Map SQL operators to PySpark operators
        operator_map = {
            'gt': '>',
            'gte': '>=',
            'lt': '<',
            'lte': '<=',
            'eq': '==',
            'neq': '!=',
            'ne': '!='
        }
        
        for op, symbol in operator_map.items():
            if op in cond_dict:
                operands = cond_dict[op]
                if type(operands) is list and len(operands) == 2:
                    left = operands[0]
                    right = operands[1]
                    
                    # Handle left operand
                    if type(left) is str:
                        # Column names can have dots for table.column notation
                        left_str = f'col("{left}")'
                    elif type(left) is dict and 'literal' in left:
                        # Literal value
                        lit_val = left['literal']
                        if type(lit_val) is str:
                            left_str = f"lit('{lit_val}')"
                        else:
                            left_str = f"lit({lit_val})"
                    elif type(left) in (int, float):
                        left_str = str(left)
                    else:
                        left_str = translate_value(left)
                    
                    # Handle right operand
                    if type(right) is str:
                        # Heuristic to distinguish column names from string literals:
                        # Column names typically: lowercase, snake_case, or table.column
                        # String literals: any case, often capitalized words
                        if '.' in right:
                            # table.column reference
                            right_str = f'col("{right}")'
                        elif '_' in right:
                            # snake_case column name
                            right_str = f'col("{right}")'
                        elif ' ' in right:
                            # Contains spaces - definitely a literal
                            right_str = f"lit('{right}')"
                        elif right.islower():
                            # All lowercase - could be column or literal
                            # Treat as literal in WHERE comparisons (common for status values)
                            right_str = f"lit('{right}')"
                        elif right[0].isupper() and len(right) > 1:
                            # Starts with uppercase - likely a literal value (e.g., "West", "Active")
                            right_str = f"lit('{right}')"
                        else:
                            # Default to column
                            right_str = f'col("{right}")'
                    elif type(right) is dict:
                        # Use updated translate_value which handles literals and arithmetic
                        right_str = translate_value(right)
                    elif type(right) in (int, float):
                        right_str = str(right)
                    else:
                        right_str = str(right)
                    
                    return f"{left_str} {symbol} {right_str}"
        
        # Fallback for complex conditions
        return str(cond_dict)
    
    def translate_value(val, prefer_literal=False):
        """Translate a value to PySpark syntax (column, literal, or number)
        
        Args:
            val: The value to translate
            prefer_literal: If True, treat ambiguous strings as literals (for CASE THEN/ELSE)
        """
        # # # print(f'translating value: {val}')
        if type(val) is str:
            # For CASE statements THEN/ELSE values, default to literals unless clearly a column
            # Heuristic: single word strings in CASE are usually literals like 'High', 'Low', 'Active'
            # Only treat as column if it looks like a typical column name (has underscore or is all lowercase)
            if prefer_literal:
                # In CASE THEN/ELSE context, treat as literal unless it looks like a column name
                if '_' in val or val.islower():
                    return f'col("{val}")'
                else:
                    return f"lit('{val}')"
            else:
                # General case: check for obvious literals (spaces, special chars)
                # Allow dots in column names (e.g. table.column)
                if ' ' in val or not val.replace('_', '').replace('.', '').isalnum():
                    return f"lit('{val}')"
                else:
                    return f'col("{val}")'
        elif type(val) in (int, float):
            return str(val)
        elif type(val) is dict:
            if 'null' in val:
                return 'lit(None)'
            if 'literal' in val:
                lit_val = val['literal']
                if type(lit_val) is str:
                    return f"lit('{lit_val}')"
                else:
                    return f"lit({lit_val})"
            
            # Check for arithmetic/functions
            func_name = list(val.keys())[0]
            
            # Use translate_function for known operators/functions
            # But we must ensure it doesn't just return str(val) fallback which created the issue
            # Let's try to recursively translate using arithmetic map from translate_function
            
            # Map SQL arithmetic to PySpark operators
            arithmetic_map = {
                'add': '+',
                'sub': '-',
                'mul': '*',
                'div': '/',
                'mod': '%'
            }
            
            if func_name in arithmetic_map:
                args = val[func_name]
                if type(args) is list and len(args) == 2:
                    left = translate_value(args[0])
                    right = translate_value(args[1])
                    return f"({left} {arithmetic_map[func_name]} {right})"
            
            # Fallback to general function translation if not caught above
            # Note: translate_function might need to be called here
            translated = translate_function(val)
            if translated:
                return translated
                
            return str(val)
        else:
            return str(val)
    
    def build_window_spec(over_spec):
        """Build a PySpark Window spec string from an OVER() dict.
        Supports keys: partitionby/partition_by, orderby/order_by, rows/range frames.
        """
        # # # print(f'building window spec: {over_spec}')
        if type(over_spec) is not dict:
            return "Window.unboundedWindow"  # fallback minimal window

        parts = []
        # Partition by
        part_keys = ['partitionby', 'partition_by', 'partition by']
        partition_val = None
        for k in part_keys:
            if k in over_spec:
                partition_val = over_spec[k]
                break
        if partition_val is not None:
            if type(partition_val) is list:
                cols = ", ".join([f'"{c}"' if type(c) is str else str(c) for c in partition_val])
            else:
                cols = f'"{partition_val}"' if type(partition_val) is str else str(partition_val)
            parts.append(f"Window.partitionBy({cols})")
        else:
            parts.append("Window")

        # Order by
        order_keys = ['orderby', 'order_by']
        order_val = None
        for k in order_keys:
            if k in over_spec:
                order_val = over_spec[k]
                break
        if order_val is not None:
            order_items = order_val if type(order_val) is list else [order_val]
            order_exprs = []
            for itm in order_items:
                if type(itm) is dict:
                    col_name = itm.get('value', itm.get('column'))
                    sort = itm.get('sort', 'asc')
                    direction = 'desc()' if str(sort).lower() == 'desc' else 'asc()'
                    order_exprs.append(f'col("{col_name}").{direction}')
                else:
                    order_exprs.append(f'col("{str(itm)}").asc()')
            parts[-1] = parts[-1] + f".orderBy({', '.join(order_exprs)})"

        # Frame (ROWS/RANGE)
        def parse_boundary(b):
            # Returns PySpark boundary expression for rowsBetween/rangeBetween
            if b is None:
                return None
            s = str(b).lower()
            if 'unbounded preceding' in s:
                return 'Window.unboundedPreceding'
            if 'unbounded following' in s:
                return 'Window.unboundedFollowing'
            if 'current row' in s:
                return '0'
            # N preceding/following
            if 'preceding' in s:
                try:
                    n = int(''.join(ch for ch in s if ch.isdigit()))
                    return str(-abs(n))
                except:
                    return '0'
            if 'following' in s:
                try:
                    n = int(''.join(ch for ch in s if ch.isdigit()))
                    return str(abs(n))
                except:
                    return '0'
            # numeric literal
            try:
                return str(int(s))
            except:
                return None

        for frame_key, api in [('rows', 'rowsBetween'), ('range', 'rangeBetween')]:
            if frame_key in over_spec:
                frame_val = over_spec[frame_key]
                start_b = end_b = None
                # Accept {'between': [start, end]} or list [start, end] or dict with 'start'/'end'
                if type(frame_val) is dict and 'between' in frame_val:
                    vals = frame_val['between']
                    if type(vals) is list and len(vals) == 2:
                        start_b, end_b = vals[0], vals[1]
                elif type(frame_val) is dict and 'start' in frame_val and 'end' in frame_val:
                    start_b, end_b = frame_val['start'], frame_val['end']
                elif type(frame_val) is dict and 'min' in frame_val and 'max' in frame_val:
                    start_b, end_b = frame_val['min'], frame_val['max']
                elif type(frame_val) is list and len(frame_val) == 2:
                    start_b, end_b = frame_val[0], frame_val[1]

                start_expr = parse_boundary(start_b)
                end_expr = parse_boundary(end_b)
                if start_expr is not None and end_expr is not None:
                    # If mo-sql-parsing uses 'range' key for ROWS BETWEEN, we might want to default to rowsBetween
                    # if the values are integers and the key is 'range'.
                    # However, strictly 'range' -> rangeBetween.
                    # Given the ambiguity, we'll stick to the key provided unless forced otherwise.
                    # But for now, just appending the api call is enough to fix the missing output.
                    
                    # Correction: if the key is 'range' but the values are clearly row offsets (integers),
                    # AND we suspect parser conflation, we might want to be careful.
                    # But without more info, we use the api derived from key.
                    # Wait, if we use rangeBetween(-2, 0) on a Date column, PySpark expects a timestamp/long
                    # or an interval. Integers might be interpreted as days?
                    # Let's assume the mapping is correct for now.
                    parts[-1] = parts[-1] + f".{api}({start_expr}, {end_expr})"
                break

        return parts[-1]

    def translate_function(func_dict):
        """Translate a function from parsed SQL structure to PySpark syntax"""
        # # # print(f'translating function: {func_dict}')
        # # # # print(func_dict)
        if type(func_dict) is not dict:
            return None
        
        # Handle window functions via OVER clause (top-level form: {'over': {...}})
        if 'over' in func_dict:
            # # # print(f'has top level over: {func_dict}')
            over_spec = func_dict['over']
            # Expect inner expression under 'value' or 'expr' (sibling or nested)
            inner_expr_spec = func_dict.get('value') or func_dict.get('expr')
            
            if inner_expr_spec is None and type(over_spec) is dict:
                inner_expr_spec = over_spec.get('value') or over_spec.get('expr')
            
            # Build inner expression
            if inner_expr_spec is None:
                # Fallback to string form
                inner_expr = str(func_dict)
            else:
                if type(inner_expr_spec) is dict:
                    inner_expr = translate_function(inner_expr_spec)
                elif type(inner_expr_spec) is str:
                    inner_expr = f'col("{inner_expr_spec}")'
                else:
                    inner_expr = str(inner_expr_spec)
            window_spec = build_window_spec(over_spec)
            return f"{inner_expr}.over({window_spec})"

        # Handle special case: CASE statements
        if 'case' in func_dict:
            case_data = func_dict['case']
            
            # Handle single WHEN without ELSE (returns dict instead of list)
            if type(case_data) is dict and 'when' in case_data and 'then' in case_data:
                case_list = [case_data]
            elif type(case_data) is list:
                case_list = case_data
            else:
                return str(func_dict)
            
            # Build when().when()...otherwise() chain
            when_clauses = []
            otherwise_value = None
            
            for item in case_list:
                if type(item) is dict and 'when' in item and 'then' in item:
                    # This is a WHEN clause
                    condition = translate_condition(item['when'])
                    then_value = translate_value(item['then'], prefer_literal=True)
                    when_clauses.append(f"when({condition}, {then_value})")
                else:
                    # This is the ELSE clause (last item without 'when')
                    otherwise_value = translate_value(item, prefer_literal=True)
            
            # Build the final expression
            if when_clauses:
                result = ".".join(when_clauses)
                if otherwise_value:
                    result += f".otherwise({otherwise_value})"
                return result
            else:
                return str(func_dict)
        
        # Handle literal values (sometimes passed as function-like dicts)
        if 'literal' in func_dict:
            lit_val = func_dict['literal']
            if type(lit_val) is str:
                return f"lit('{lit_val}')"
            else:
                return f"lit({lit_val})"
        
        # Handle CAST
        if 'cast' in func_dict:
            # Identify format
            expr = None
            target_type = None
            
            # Case 1: {'cast': expr, 'as': type} (top level keys)
            if 'as' in func_dict:
                expr = func_dict['cast']
                target_type = func_dict['as']
            else:
                val = func_dict['cast']
                # Case 2: {'cast': {'value': expr, 'as': type}} or {'to': type}
                if type(val) is dict:
                    expr = val.get('value')
                    target_type = val.get('as') or val.get('to')
                # Case 3: {'cast': [expr, type]} (if parsed that way)
                elif type(val) is list and len(val) == 2:
                    expr = val[0]
                    target_type = val[1]
            
            if expr is not None and target_type is not None:
                # Translate expression
                expr_str = translate_value(expr)
                
                # Stringify target type
                # It might be a dict like {'decimal': [12, 2]} or string 'int'
                if type(target_type) is dict:
                    # Reconstruct type string
                    k = list(target_type.keys())[0]
                    v = target_type[k]
                    if type(v) is list:
                        type_str = f"{k}({', '.join(str(x) for x in v)})"
                    else:
                        type_str = f"{k}({v})"
                else:
                    type_str = str(target_type)
                
                return f'{expr_str}.cast("{type_str}")'

        # Get the function name and arguments
        func_name = list(func_dict.keys())[0]
        func_args = func_dict[func_name]
        
        # Convert function name to lowercase for PySpark
        # # # # print(func_name)
        if func_name in SPECIAL_FUNCTION_MAPPING:
            pyspark_func = SPECIAL_FUNCTION_MAPPING[func_name]
        else:
            pyspark_func = func_name.lower()
        
        # Map SQL arithmetic to PySpark operators
        arithmetic_map = {
            'add': '+',
            'sub': '-',
            'mul': '*',
            'div': '/',
            'mod': '%'
        }
        
        # Handle function-as-window form: {'sum': {'over': {...}, 'value': 'col'}}
        if type(func_args) is dict and 'over' in func_args:
            # Build base function call using its 'value' or first positional if present
            value_arg = func_args.get('value') or func_args.get('expr')
            if value_arg is None and 'args' in func_args:
                # nonstandard, try list under 'args'
                args_list = func_args['args'] if type(func_args['args']) is list else [func_args['args']]
                value_arg = args_list[0] if args_list else None
            if value_arg is None:
                base_call = f"{pyspark_func}()"
            else:
                base_call = f'{pyspark_func}({translate_value(value_arg)})'
            # # # print(f'has function-as-window form: {func_args}\n base_call: {base_call}')
            window_spec = build_window_spec(func_args['over'])
            return f"{base_call}.over({window_spec})"

        # Handle arguments
        if type(func_args) is str:
            # Single column argument
            return f'{pyspark_func}(col("{func_args}"))'
        elif type(func_args) is list:
            # Multiple arguments - need to process each
            arg_strs = []
            for arg in func_args:
                if type(arg) is str:
                    # Distinguish between column names and string literals
                    # If string contains spaces, special chars, or is very short (like ' '), treat as literal
                    # Otherwise treat as column
                    # Allow dots in column names (e.g. table.column)
                    if ' ' in arg or len(arg.strip()) == 0 or not arg.replace('_', '').replace('.', '').isalnum():
                        # String literal
                        arg_strs.append(f"lit('{arg}')")
                    else:
                        # Column reference
                        arg_strs.append(f'col("{arg}")')
                elif type(arg) is dict:
                    if 'literal' in arg:
                        # Literal value
                        lit_val = arg['literal']
                        if type(lit_val) is str:
                            arg_strs.append(f"lit('{lit_val}')")
                        else:
                            arg_strs.append(f"lit({lit_val})")
                    else:
                        # Nested function - recursively translate
                        nested = translate_function(arg)
                        if nested:
                            arg_strs.append(nested)
                        else:
                            # If translation fails, convert to string
                            arg_strs.append(str(arg))
                elif type(arg) in (int, float):
                    # Numeric literal
                    arg_strs.append(str(arg))
                else:
                    arg_strs.append(str(arg))
            
            if func_name in arithmetic_map and len(arg_strs) == 2:
                return f"({arg_strs[0]} {arithmetic_map[func_name]} {arg_strs[1]})"
            
            return f'{pyspark_func}({", ".join(arg_strs)})'
        elif type(func_args) is dict:
            if not func_args:
                return f"{pyspark_func}()"
            # Complex structure - try SQL format
            try:
                return format({"select": func_dict}).replace("SELECT ", "")
            except:
                return str(func_dict)
        else:
            # Fallback
            return f'{pyspark_func}(col("{str(func_args)}"))'


    def fn_from(value):
        # # # print(f'translating from: {value}')
        result_from=""
        if type(value) is str:
            result_from = format({ "from": value })
            result_from = result_from[5:]
        elif type(value) is dict:
            # Check for direct set operation (union/intersect etc) in FROM
            # e.g. from: {'union_all': [...]}
            if _find_set_op_key(value):
                result_from = f"({fn_genSQL_or_set(value)})"
            # Check if this is a subquery in FROM (derived table)
            elif "name" in value.keys() and "value" in value.keys() and type(value['value']) is dict:
                # This is a subquery with an alias
                subquery_sql = fn_genSQL_or_set(value['value'])
                result_from = f"({subquery_sql}).alias(\"{value['name']}\")"
            elif "name" in value.keys():
                result_from = result_from + value['value']+".alias(\""+value['name']+"\")"
            else:
                # Fallback: hope it has 'value'
                val = value.get('value', '')
                result_from = result_from + str(val) + ""
        elif type(value) is list:
            # Handle JOINs and comma-separated tables (implicit cross joins)
            # We want to build a chain: table1.crossJoin(table2).join(table3, ...)
            
            tables = []
            joins = []
            
            # Helper to process a table item (str or dict) into a PySpark DataFrame string
            def process_table_item(item):
                if type(item) is str:
                    return item
                elif type(item) is dict:
                    if "name" in item and "value" in item:
                         # Check for subquery
                        if type(item['value']) is dict:
                             subq = fn_genSQL_or_set(item['value'])
                             return f"({subq}).alias(\"{item['name']}\")"
                        return f'{item["value"]}.alias("{item["name"]}")'
                    elif "value" in item:
                        # Handle case where value is a dict (subquery) but no alias
                        # Or simple table name in dict wrapper
                        val = item['value']
                        if type(val) is dict:
                            # Unaliased subquery in FROM list
                            # PySpark requires alias for subquery
                            subq = fn_genSQL_or_set(val)
                            # Generate a random alias if missing
                            return f"({subq}).alias(\"subq_{id(val) % 1000}\")"
                        return str(val)
                    
                    # Fallback for other dict structures in FROM (e.g. unhandled types)
                    # Check if it's a set operation dict
                    if _find_set_op_key(item):
                        # It's a set operation directly in the FROM list
                        # PySpark requires alias for subqueries/sets in join
                        subq = fn_genSQL_or_set(item)
                        return f"({subq}).alias(\"subq_{id(item) % 1000}\")"
                        
                    # Don't return None, return string representation to avoid crossJoin(None)
                    # # print(f"DEBUG: process_table_item fallback for: {item}")
                    return str(item)
                return str(item)

            # First pass: Separate base tables from explicit joins
            base_items = []
            explicit_join_items = []
            
            has_explicit_joins = False
            for item in value:
                if type(item) is dict and any(k in item for k in ['join', 'inner join', 'left join', 'right join', 'full outer join', 'cross join']):
                    has_explicit_joins = True
                    explicit_join_items.append(item)
                else:
                    base_items.append(item)
            
            # Process base items (comma separated -> cross joins)
            if not base_items and not explicit_join_items:
                return ""
            
            # The first item is the start of the chain
            if base_items:
                first = base_items[0]
                result_from = process_table_item(first)
                
                # Subsequent base items are cross joins
                for item in base_items[1:]:
                    table_str = process_table_item(item)
                    result_from += f".crossJoin({table_str})"
            else:
                # No base items? (unlikely in valid SQL, maybe starts with a JOIN?)
                # Just take the table from the first join
                # This is edge case handling
                pass

            # Process explicit joins
            for item in explicit_join_items:
                join_type = None
                join_table_raw = None
                join_condition = None
                
                if 'join' in item:
                    join_type = 'inner'
                    join_table_raw = item['join']
                elif 'inner join' in item:
                    join_type = 'inner'
                    join_table_raw = item['inner join']
                elif 'left join' in item:
                    join_type = 'left'
                    join_table_raw = item['left join']
                elif 'right join' in item:
                    join_type = 'right'
                    join_table_raw = item['right join']
                elif 'full outer join' in item:
                    join_type = 'outer'
                    join_table_raw = item['full outer join']
                elif 'cross join' in item:
                    join_type = 'cross'
                    join_table_raw = item['cross join']
                
                if join_type:
                    # Parse table
                    join_table_str = ""
                    if type(join_table_raw) is dict:
                        if "name" in join_table_raw:
                             if "value" in join_table_raw and type(join_table_raw['value']) is dict:
                                 subq = fn_genSQL_or_set(join_table_raw['value'])
                                 join_table_str = f"({subq}).alias(\"{join_table_raw['name']}\")"
                             else:
                                 join_table_str = f'{join_table_raw["value"]}.alias("{join_table_raw["name"]}")'
                        else:
                             join_table_str = str(join_table_raw)
                    else:
                        join_table_str = str(join_table_raw)

                    # Parse condition
                    if 'on' in item:
                        join_condition = translate_condition(item['on'])
                    
                    if join_type == 'cross':
                        result_from += f".crossJoin({join_table_str})"
                    else:
                        result_from += f".join({join_table_str}, {join_condition}, '{join_type}')"
                
        return result_from
            

    def fn_select(value, outer_alias=None, scalar_subq_list=None, agg_aliases=None):
        # print('\n')
        # print(f'FN_SELECT: value: {value}')
        # print(f'FN_SELECT: outer_alias: {outer_alias}')
        # print(f'FN_SELECT: scalar_subq_list: {scalar_subq_list}')
        # print(f'FN_SELECT: agg_aliases: {agg_aliases}')
        # print('\n')
        # # print(f'translating select: {value} \n')
        # # print(f'outer_alias: {outer_alias}')
        # # print(f'scalar_subq_list: {scalar_subq_list}')
        # # print(f'agg_aliases: {agg_aliases}')
        result_select=""
        
        # Check if we should qualify columns (only if outer_alias exists and we have scalar subqueries)
        has_outer_alias = outer_alias is not None
        has_scalar_subqueries = scalar_subq_list is not None and len(scalar_subq_list) > 0
        
        if type(value) is str:
            # Simple column - do NOT auto-qualify, user input should be respected
            result_select = result_select + "\""+value+"\","
        elif type(value) is dict:
            # Handle SELECT * case
            if "all_columns" in value.keys():
                result_select = "\"*\""
            elif "name" in value.keys():
                # Check if it's an aggregate function (top-level)
                if type(value['value']) is dict and is_aggregate_function(value['value']) and not is_window_expression(value):
                    # Aggregate function with alias - just use the alias name
                    result_select = result_select + "\""+value['name']+"\","
                elif type(value['value']) is dict and 'select' in value['value']:
                    # Scalar subquery - will be handled by JOIN in fn_genSQL
                    # Find the corresponding scalar subquery info to get the agg_alias
                    agg_alias = f"{value['name']}_agg"
                    # Qualify the column with the aggregation alias
                    result_select = result_select + f'coalesce(col("{agg_alias}.{value["name"]}"), lit(0)).alias("{value["name"]}"),'
                elif type(value['value']) is dict:
                    # Scalar function with alias - translate to PySpark
                    # Use alias replacement for any aggregates inside the function
                    mod_expr = replace_aggregates_with_aliases(value['value'], agg_aliases)
                    if type(mod_expr) is str:
                        func_str = f'col("{mod_expr}")'
                    else:
                        func_str = translate_function(mod_expr)
                        
                    if 'over' in value:
                        func_str = func_str + f".over({build_window_spec(value['over'])})"
                    result_select = result_select + func_str + ".alias(\""+value['name']+"\"),"
                else:
                    # Regular column with alias
                    result_select = result_select + f'col("{value["value"]}").alias("{value["name"]}"),'
            elif "value" in value.keys() and type(value['value']) is dict:
                if is_aggregate_function(value['value']) and not is_window_expression(value):
                    # Aggregate function - skip, handled by fn_agg
                    # BUT we must select the alias generated by fn_agg
                    if agg_aliases:
                        agg_str = get_agg_key(value['value'])
                        if agg_str in agg_aliases:
                            result_select = result_select + "\""+agg_aliases[agg_str]+"\","
                        else:
                            # Fallback if strict string matching fails (shouldn't happen with updated fn_agg)
                            pass
                    else:
                        pass
                elif 'select' in value['value']:
                    # This is a scalar subquery - not directly supported in PySpark
                    # Would need to be converted to a join or window function
                    result_select = result_select + "\"# SUBQUERY\","
                else:
                    # Scalar function without alias
                    # Use alias replacement for any aggregates inside the function
                    mod_expr = replace_aggregates_with_aliases(value['value'], agg_aliases)
                    if type(mod_expr) is str:
                        func_str = f'col("{mod_expr}")'
                    else:
                        func_str = translate_function(mod_expr)
                        
                    # Handle parent-level OVER on this select item
                    if 'over' in value:
                        func_str = func_str + f".over({build_window_spec(value['over'])})"
                    result_select = result_select + func_str + ","
            else:
                if 'value' in value:
                    result_select = result_select + "\""+value['value']+"\""
                else:
                    # Fallback for complex expressions/conditions in select
                    # e.g. case when, or boolean conditions
                    res = translate_condition(value)
                    if res:
                         result_select = result_select + res + ","
                    else:
                         result_select = result_select + str(value) + ","
        elif type(value) is list:
            for item_select in value:
                if type(item_select) is dict:
                    if type(item_select['value']) is dict:
                        if is_aggregate_function(item_select['value']) and not is_window_expression(item_select):
                            # Aggregate function
                            if "name" in item_select.keys():
                                result_select = result_select + "\""+item_select['name']+"\","
                            else:
                                # No explicit alias - use the one generated by fn_agg
                                if agg_aliases:
                                    agg_str = get_agg_key(item_select['value'])
                                    if agg_str in agg_aliases:
                                        result_select = result_select + "\""+agg_aliases[agg_str]+"\","
                                else:
                                    pass
                        elif 'select' in item_select['value']:
                            # Scalar subquery - will be handled by JOIN in fn_genSQL
                            # Just reference the alias name that will be created by the join
                            if "name" in item_select.keys():
                                # The join will create a column with this alias name
                                # Qualify with the aggregation alias
                                agg_alias = f"{item_select['name']}_agg"
                                result_select = result_select + f'coalesce(col("{agg_alias}.{item_select["name"]}"), lit(0)).alias("{item_select["name"]}"),'
                            else:
                                result_select = result_select + f"\"# SUBQUERY\","
                        elif is_window_expression(item_select):
                            # Use alias replacement for any aggregates inside the window expression arguments
                            mod_expr = replace_aggregates_with_aliases(item_select['value'], agg_aliases)
                            if type(mod_expr) is str:
                                func_str = f'col("{mod_expr}")'
                            else:
                                func_str = translate_function(mod_expr)
                                
                            # Handle parent-level OVER on this select item
                            func_str = func_str + f".over({build_window_spec(item_select['over'])})"
                            if "name" in item_select:
                                func_str = func_str + f'.alias("{item_select["name"]}")'
                            result_select = result_select + func_str + ","
                        else:
                            # Scalar function
                            # Use alias replacement for any aggregates inside the function
                            mod_expr = replace_aggregates_with_aliases(item_select['value'], agg_aliases)
                            if type(mod_expr) is str:
                                func_str = f'col("{mod_expr}")'
                            else:
                                func_str = translate_function(mod_expr)
                                
                            if "name" in item_select.keys():
                                result_select = result_select + func_str + ".alias(\""+item_select['name']+"\"),"
                            else:
                                result_select = result_select + func_str + ","
                    else:
                        # Regular column or function
                        if "name" in item_select.keys():
                            # Column with alias
                            result_select = result_select + f'col("{item_select["value"]}").alias("{item_select["name"]}"),'
                        else:
                            # Column without alias
                            if 'over' in item_select and type(item_select['value']) is dict:
                                mod_expr = replace_aggregates_with_aliases(item_select['value'], agg_aliases)
                                if type(mod_expr) is str:
                                    func_str = f'col("{mod_expr}")'
                                else:
                                    func_str = translate_function(mod_expr)
                                func_str = func_str + f".over({build_window_spec(item_select['over'])})"
                                result_select = result_select + func_str + ","
                            else:
                                result_select = result_select + "\""+item_select['value']+"\"," 
        return result_select[:-1] if result_select.endswith(",") else result_select

    def extract_in_subqueries(condition):
        """
        Recursively extract IN subqueries from a WHERE condition tree.
        Returns (filter_expression_string, list_of_in_subqueries)
        """
        if type(condition) is not dict:
            return translate_condition(condition), []
            
        # Handle AND - allow splitting
        if 'and' in condition:
            sub_conditions = condition['and']
            if type(sub_conditions) is list:
                filters = []
                subqs = []
                for sub in sub_conditions:
                    f_str, s_list = extract_in_subqueries(sub)
                    if f_str:
                        filters.append(f"({f_str})")
                    subqs.extend(s_list)
                
                # Check if we have filters to join
                combined_filter = " & ".join(filters) if filters else None
                return combined_filter, subqs
        
        # Handle IN with subquery
        if 'in' in condition:
            in_val = condition['in']
            if type(in_val) is list and len(in_val) == 2:
                column = in_val[0]
                possible_subq = in_val[1]
                if type(possible_subq) is dict and ('select' in possible_subq or 'select_distinct' in possible_subq):
                    return None, [{
                        'type': 'in_subquery',
                        'column': column,
                        'subquery': possible_subq
                    }]
        
        # Handle everything else (OR, NOT, comparisons) using standard translation
        return translate_condition(condition), []

    def fn_where(value):
        """
        Translate WHERE clause to PySpark.
        Handles simple conditions and IN subqueries.
        Returns a dictionary with 'filters' (string) and 'subqueries' (list).
        """
        # # # print(f'translating where: {value}')
        
        filters, subqueries = extract_in_subqueries(value)
        
        return {
            'filters': filters,
            'subqueries': subqueries
        }


    def fn_groupby(value):
        # # # print(f'translating groupby: {value}')
        
        def process_group_item(item):
            if type(item) is dict:
                if 'value' in item:
                    val = item['value']
                    # Check for simple column vs expression
                    if type(val) is str:
                         # Use col() if it looks like a column, otherwise literal? 
                         # Actually groupBy usually takes strings or col objects.
                         # Safest is probably strings if simple, or col() expressions.
                         return f'"{val}"'
                    else:
                        # Complex expression
                        return translate_value(val)
                return str(item)
            elif type(item) is str:
                return f'"{item}"'
            else:
                return str(item)

        items = []
        if type(value) is list:
            for item in value:
                items.append(process_group_item(item))
        else:
            items.append(process_group_item(value))
            
        return ", ".join(items)
    
    def fn_having(value, agg_aliases=None):
        """
        Translate HAVING clause to PySpark, replacing aggregate functions with their aliases
        and converting to PySpark column expressions.
        """
        # # # print(f'translating having: {value}')
        def replace_agg_in_structure(obj):
            """Recursively replace aggregate functions with column references"""
            if type(obj) is dict:
                # Check if this is an aggregate function
                if is_aggregate_function(obj):
                    # Check if we have an alias for this aggregate
                    # Use string representation as key (matches fn_agg)
                    agg_str = get_agg_key(obj)
                    if agg_aliases and agg_str in agg_aliases:
                        return agg_aliases[agg_str]
                    
                    return obj
                else:
                    # Recursively process dictionary values
                    return {k: replace_agg_in_structure(v) for k, v in obj.items()}
            elif type(obj) is list:
                return [replace_agg_in_structure(item) for item in obj]
            else:
                return obj
        
        # Replace aggregate functions in the structure
        modified_value = replace_agg_in_structure(value)
        
        # Try to translate to PySpark column expressions
        if type(modified_value) is dict:
            translated = translate_condition(modified_value)
            if translated and translated != str(modified_value):
                return translated
        
        # Fallback: use SQL format
        result_having_sql = format({ "having": modified_value })[7:]
        return result_having_sql

    
    def get_agg_key(agg_dict):
        """Generate a consistent key for an aggregation dictionary."""
        # # print(f"DEBUG: Generating key for {agg_dict}")
        # # print(f"DEBUG: Key: {json.dumps(agg_dict, sort_keys=True)}")
        return json.dumps(agg_dict, sort_keys=True)

    def fn_agg(data, agg_aliases):
        # v_parse = parse(query)
        # print(f'\n\n fn_agg: {data}')
        v_agg = ""
        # agg_aliases = {}  # Map from aggregate expression string to alias
        
        if "select" not in data and "select_distinct" not in data:
            return "", {}
        
        select_items = data.get("select") or data.get("select_distinct")
        if type(select_items) is dict:
            select_items = [select_items]
        
        seen_aggs = set()
        
        for i in select_items:
            # Handle item value
            val = i.get("value") if type(i) is dict else i
            
            # Determine if this item IS itself a top-level aggregate (for explicit aliasing)
            top_level_agg = None
            if type(i) is dict and "value" in i and type(i["value"]) is dict:
                 if is_aggregate_function(i["value"]) and not is_window_expression(i["value"]):
                      top_level_agg = i["value"]
            
            # Find all aggregates in this expression (nested or top level)
            aggs = collect_aggregates(val)
            
            for agg in aggs:
                # Key for deduplication/lookup
                agg_str = get_agg_key(agg) # Consistent JSON string
                if agg_str in seen_aggs:
                    continue
                
                seen_aggs.add(agg_str)
                
                # Special-case combined COUNT + DISTINCT
                if 'count' in agg and 'distinct' in agg:
                    func_name = 'countDistinct'
                    func_arg = agg['count']
                else:
                    func_name = list(agg.keys())[0]
                    func_arg = agg[func_name]
                
                # Determine Alias
                alias_name = None
                # If this specific agg object matches the top-level value, use the user's alias if available
                # Use strict object comparison or key comparison
                if top_level_agg is not None and get_agg_key(top_level_agg) == agg_str and "name" in i:
                    alias_name = i["name"]
                else:
                    # Auto alias
                    if type(func_arg) is str:
                         clean = func_arg.replace("*", "star").replace(".", "_")
                         alias_name = f"{func_name.lower()}_{clean}"
                    else:
                         # Use hash for complex expression
                         alias_name = f"{func_name.lower()}_expr_{abs(hash(agg_str)) % 10000}"
                
                # Generate PySpark code
                if type(func_arg) is dict:
                    arg_translated = translate_value(func_arg)
                else:
                    arg_translated = f'col("{func_arg}")' if type(func_arg) is str and func_arg != "*" else f'lit(1)' if func_arg == "*" else str(func_arg)
                
                v_agg += (
                    f"countDistinct({arg_translated}).alias('{alias_name}')," if func_name.lower() == 'countdistinct' 
                    else f"{func_name}({arg_translated}).alias('{alias_name}'),"
                )
                agg_aliases[agg_str] = alias_name
                (f"DEBUG: Stored alias for {agg_str} -> {alias_name}")
            # print(f"DEBUG: agg_aliases: {agg_aliases}")
        
        return v_agg[:-1] if v_agg else "", agg_aliases


    def fn_orderby(query):
        # # # print(f'translating orderby: {query}')
        v_parse = parse(query)
        v_orderby_collist=""
        v_orderby = v_parse["orderby"]
        
        # Handle both single column (dict) and multiple columns (list)
        if type(v_orderby) is dict:
            v_orderby = [v_orderby]
        
        for i in v_orderby:
            if i.get("sort", "asc") == "desc":
                v_sortorder = "desc()"
            else:
                v_sortorder = "asc()"
            
            val = i.get("value", "")
            if type(val) is dict:
                val_str = translate_function(val)
            else:
                val_str = f'col("{val}")'
            
            v_orderby_collist = v_orderby_collist + val_str + "." +v_sortorder+","
        return v_orderby_collist[:-1]


    def fn_limit(query):
        # # # print(f'translating limit: {query}')
        v_parse = parse(query)
        v_limit = v_parse["limit"]
        return v_limit


    def fn_genSQL(data, agg_aliases=None):
        # print('\n')
        # print(f'ENTRYPOINT fn_genSQL: {data}')
        # print(f'ENTRYPOINT agg_aliases: {agg_aliases}')
        # print('\n')
        # if type(data) is list:
            # # print('data is a list, length: ', len(data))
            # # # print(data[-1])
        v_fn_from = v_fn_where = v_fn_groupby = v_fn_agg = v_fn_select = v_fn_orderby = v_fn_limit = v_fn_having = ""
        v_fn_distinct = False
        has_aggregate = False
        scalar_subqueries_where = []  # Subqueries to join before WHERE
        scalar_subqueries_having = [] # Subqueries to join after AGGREGATION (before HAVING)
        use_spark_sql_fallback = False
        outer_table_aliases = []  # List of all outer table aliases
        outer_table_alias = None  # Single primary alias (legacy)
        if agg_aliases is None:
            agg_aliases = {}
        # agg_aliases = {}  # Store aggregate function aliases for HAVING clause
        
        # Extract outer table aliases
        if "from" in data:
            from_value = data["from"]
            if type(from_value) is dict:
                if 'name' in from_value:
                    outer_table_alias = from_value['name']
                    outer_table_aliases.append(from_value['name'])
                elif 'value' in from_value:
                    # If single table without alias, maybe table name is alias?
                    pass 
            elif type(from_value) is str:
                # Table name itself might be used for qualification, but aliases are preferred
                pass
            elif type(from_value) is list:
                # Multiple tables
                for item in from_value:
                    if type(item) is dict and 'name' in item:
                        outer_table_aliases.append(item['name'])
                # If list, outer_table_alias (single) is ambiguous/first one?
                if outer_table_aliases:
                    outer_table_alias = outer_table_aliases[0]
        
        # Helper to process scalar subqueries in expressions (SELECT/WHERE/HAVING)
        def process_subqueries(expr, scalar_subqs):
            if type(expr) is list:
                return [process_subqueries(i, scalar_subqs) for i in expr]
            
            if type(expr) is not dict:
                return expr
            
            # Check if this node IS a subquery (or EXISTS subquery)
            if 'select' in expr or 'select_distinct' in expr:
                 alias_name = f"subq_{len(scalar_subqs)}_{id(expr) % 1000}"
                 # Pass list of outer aliases for better correlation detection
                 subq_info = translate_scalar_subquery_join(expr, alias_name, outer_table_aliases)
                 if subq_info:
                     scalar_subqs.append(subq_info)
                     agg_alias = f"{alias_name}_agg"
                     # Return the column reference that will be valid after join
                     return f"{agg_alias}.{alias_name}"
                 return expr
                 
            # Check for EXISTS subquery - usually {'exists': {select...}}
            if 'exists' in expr and type(expr['exists']) is dict and ('select' in expr['exists'] or 'select_distinct' in expr['exists']):
                 # This is an EXISTS subquery.
                 # We translate it similarly to a scalar subquery, but we want to check for existence (non-null).
                 subquery = expr['exists']
                 alias_name = f"exists_{len(scalar_subqs)}_{id(subquery) % 1000}"
                 subq_info = translate_scalar_subquery_join(subquery, alias_name, outer_table_aliases)
                 if subq_info:
                     scalar_subqs.append(subq_info)
                     agg_alias = f"{alias_name}_agg"
                     # Return PySpark boolean expression string
                     return f"col('{agg_alias}.{alias_name}').isNotNull()"
                 else:
                     #  # # print(f"DEBUG: Failed to translate EXISTS subquery: {subquery}")
                     pass
                 return expr

            # Recurse
            new_expr = {}
            for k, v in expr.items():
                # Skip recursion for IN/NOT IN subqueries - they are handled separately in WHERE
                # We do NOT skip 'exists' anymore to support EXISTS subqueries via join
                
                if k in ['in', 'nin', 'not in']:
                    # Check if 'in' value is subquery - handled by fn_where logic
                    new_expr[k] = v
                    continue
                
                if type(v) is list:
                    new_expr[k] = [process_subqueries(i, scalar_subqs) for i in v]
                elif type(v) is dict:
                    new_expr[k] = process_subqueries(v, scalar_subqs)
                else:
                    new_expr[k] = v
            return new_expr

        # First pass: Recursively process scalar subqueries in SELECT clause
        # This handles subqueries nested in CASE, functions, etc.
        if "select" in data:
            data['select'] = process_subqueries(data['select'], scalar_subqueries_where)
        if "select_distinct" in data:
            data['select_distinct'] = process_subqueries(data['select_distinct'], scalar_subqueries_where)

        # Process scalar subqueries in WHERE clause
        if "where" in data:
            where_val = data["where"]
            # Process and update the WHERE clause in data
            data['where'] = process_subqueries(where_val, scalar_subqueries_where)

        # Process scalar subqueries in HAVING clause
        if "having" in data:
            having_val = data["having"]
            # Use separate list for HAVING subqueries
            data['having'] = process_subqueries(having_val, scalar_subqueries_having)
        
        # If fallback is needed, return spark.sql() wrapper
        if use_spark_sql_fallback:
            sql_query = format(data)
            return f'spark.sql("""{sql_query}""")'
        
        # Check if SELECT has aggregate functions (not just any function). Skip window expressions.
        # Note: after process_subqueries, subqueries are replaced by strings.
        # We need to check for aggregates in the modified structure.
        select_value = data.get("select") or data.get("select_distinct")
        if select_value:
            if type(select_value) is dict and "value" in select_value:
                has_aggregate = (is_aggregate_function(select_value["value"]) and not is_window_expression(select_value))
            elif type(select_value) is list:
                for item in select_value:
                    if type(item) is dict and "value" in item:
                        if is_aggregate_function(item["value"]) and not is_window_expression(item):
                            has_aggregate = True
                            break
        
        # for key,value in data.items():
        #     # handle from
        #     if str(key)=="from":
        #         v_fn_from = fn_from(value)

        #     #handle where
        #     if str(key) =="where":
        #         v_fn_where = fn_where(value)

        #     #handle groupby
        #     if str(key) =="groupby":
        #         v_fn_groupby = fn_groupby(value)

        #     #handle agg - call if there's a groupby OR if there are aggregate functions (excluding windowed aggs)
        #     if str(key) =="groupby" or ((str(key) == "select" or str(key) == "select_distinct") and has_aggregate):
        #         # FIX: pass data, not query
        #         v_fn_agg, agg_aliases = fn_agg(data, agg_aliases)
        #         # # print(f"DEBUG: v_fn_agg: {v_fn_agg}")
        #         # # print(f"DEBUG: agg_aliases: {agg_aliases}")
            
        #     #handle having
        #     if str(key) =="having":
        #         v_fn_having = fn_having(value, agg_aliases)

        #     #handle select
        #     if str(key) =="select":
        #         # # print(f"DEBUG: OUTSIDE fn_select: {value}")
        #         # # print(f"DEBUG: OUTSIDE outer_table_alias: {outer_table_alias}")
        #         # # print(f"DEBUG: OUTSIDE scalar_subqueries_where: {scalar_subqueries_where}")
        #         # # print(f"DEBUG: OUTSIDE scalar_subqueries_having: {scalar_subqueries_having}")
        #         # # print(f"DEBUG: OUTSIDE agg_aliases: {agg_aliases}")
        #         # Pass scalar_subqueries_where for qualification if needed (legacy behavior)
        #         # Actually fn_select uses it to decide if it should qualify columns.
        #         # We should probably pass both or just check if any exist.
        #         all_scalar_subqs = scalar_subqueries_where + scalar_subqueries_having
        #         v_fn_select = fn_select(value, outer_table_alias, all_scalar_subqs, agg_aliases)
            
        #     #handle select_distinct
        #     if str(key) =="select_distinct":
        #         all_scalar_subqs = scalar_subqueries_where + scalar_subqueries_having
        #         v_fn_select = fn_select(value, outer_table_alias, all_scalar_subqs, agg_aliases)
        #         v_fn_distinct = True

        #     #handle sort
        #     if str(key) =="orderby":
        #         v_fn_orderby = fn_orderby(query) # Still using query for orderby? Check fn_orderby implementation

        #     #handle limit
        #     if str(key) =="limit":
        #         v_fn_limit = fn_limit(query) # Still using query for limit?

        
        # handle from
        if "from" in data:
            v_fn_from = fn_from(data["from"])

        #handle where
        if "where" in data:
            v_fn_where = fn_where(data["where"])

        #handle groupby
        if "groupby" in data:
            v_fn_groupby = fn_groupby(data["groupby"])

        #handle agg - call if there's a groupby OR if there are aggregate functions (excluding windowed aggs)
        if "groupby" in data or (("select" in data or "select_distinct" in data) and has_aggregate):
            # FIX: pass data, not query
            v_fn_agg, agg_aliases = fn_agg(data, agg_aliases)
            # # print(f"DEBUG: v_fn_agg: {v_fn_agg}")
            # # print(f"DEBUG: agg_aliases: {agg_aliases}")
        
        #handle having
        if "having" in data:
            v_fn_having = fn_having(data["having"], agg_aliases)

        #handle select
        if "select" in data:
            # # print(f"DEBUG: OUTSIDE fn_select: {value}")
            # # print(f"DEBUG: OUTSIDE outer_table_alias: {outer_table_alias}")
            # # print(f"DEBUG: OUTSIDE scalar_subqueries_where: {scalar_subqueries_where}")
            # # print(f"DEBUG: OUTSIDE scalar_subqueries_having: {scalar_subqueries_having}")
            # # print(f"DEBUG: OUTSIDE agg_aliases: {agg_aliases}")
            # Pass scalar_subqueries_where for qualification if needed (legacy behavior)
            # Actually fn_select uses it to decide if it should qualify columns.
            # We should probably pass both or just check if any exist.
            all_scalar_subqs = scalar_subqueries_where + scalar_subqueries_having
            v_fn_select = fn_select(data["select"], outer_table_alias, all_scalar_subqs, agg_aliases)
        
        #handle select_distinct
        if "select_distinct" in data:
            all_scalar_subqs = scalar_subqueries_where + scalar_subqueries_having
            v_fn_select = fn_select(data["select_distinct"], outer_table_alias, all_scalar_subqs, agg_aliases)
            v_fn_distinct = True

        #handle sort
        if "orderby" in data:
            v_fn_orderby = fn_orderby(query) # Still using query for orderby? Check fn_orderby implementation

        #handle limit
        if "limit" in data:
            v_fn_limit = fn_limit(query) # Still using query for limit?

        # Define helper to append scalar subquery joins
        def append_scalar_joins(stmt, subqs):
            for subq_info in subqs:
                # Build the aggregation DataFrame
                inner_table = subq_info['inner_table']
                inner_alias = subq_info['inner_alias']
                correlation_column = subq_info['correlation_column']
                outer_column = subq_info['outer_column']
                alias_name = subq_info['alias_name']
                other_filters = subq_info.get('other_filters', [])
                
                # Apply filters to inner table
                if other_filters:
                    filter_str = ""
                    for cond in other_filters:
                        translated_cond = translate_condition(cond)
                        if translated_cond:
                             if filter_str:
                                 filter_str += f" & ({translated_cond})"
                             else:
                                 filter_str = f"({translated_cond})"
                    
                    if filter_str:
                        # Ensure we don't double wrap if inner_table is already an expression string
                        if not inner_table.endswith(")"):
                             # It might be a table name 'date_dim'
                             inner_table = f"{inner_table}.filter({filter_str})"
                        else:
                             # It might be '(subquery)'
                             inner_table = f"{inner_table}.filter({filter_str})"
                
                # Use the full aggregation expression
                agg_expr_str = subq_info.get('full_agg_expr')
                if not agg_expr_str:
                    # Fallback
                    func = subq_info['agg_func']
                    col_name = subq_info['agg_column']
                    agg_expr_str = f'{func}(col("{col_name}"))' if col_name != "*" else f'{func}(lit(1))'
            
                # Build the join
                agg_alias = f"{alias_name}_agg"
                
                # Build join - using LEFT join to preserve all outer rows
                if correlation_column and outer_column:
                    # Format: .join(inner_table.groupBy("correlation_col").agg(...), condition, 'left')
                    stmt += f'\\\n.join({inner_table}.alias("{inner_alias}").groupBy("{correlation_column}").agg({agg_expr_str}.alias("{alias_name}")).alias("{agg_alias}"), col("{outer_column}") == col("{agg_alias}.{correlation_column}"), "left")'
                else:
                    # Uncorrelated scalar subquery (single row)
                    # Use cross join
                    stmt += f'\\\n.crossJoin({inner_table}.alias("{inner_alias}").agg({agg_expr_str}.alias("{alias_name}")).alias("{agg_alias}"))'
            return stmt

        v_final_stmt = ""
        if v_fn_from:
            v_final_stmt = v_final_stmt + v_fn_from
        
        # Add scalar subquery joins for WHERE clause (before WHERE)
        v_final_stmt = append_scalar_joins(v_final_stmt, scalar_subqueries_where)
        
        # Handle WHERE clause
        if v_fn_where:
            # Handle new structure from fn_where: {'filters': str, 'subqueries': list}
            if type(v_fn_where) is dict and 'subqueries' in v_fn_where:
                # 1. Handle subquery joins (semi-joins for IN clause)
                for subq_item in v_fn_where['subqueries']:
                    if subq_item.get('type') == 'in_subquery':
                        outer_column = subq_item['column']
                        subquery_data = subq_item['subquery']
                        
                        # Extract the inner column from the subquery's SELECT
                        inner_column = None
                        if 'select' in subquery_data:
                            select_data = subquery_data['select']
                            if type(select_data) is dict:
                                if 'value' in select_data:
                                    inner_column = select_data['value']
                                elif 'all_columns' in select_data:
                                    # IN (SELECT *) usually implies SELECT * from single column or tuple
                                    pass
                            elif type(select_data) is list and len(select_data) > 0:
                                if type(select_data[0]) is dict and 'value' in select_data[0]:
                                    inner_column = select_data[0]['value']
                        
                        # Translate the subquery
                        subquery_pyspark = fn_genSQL(subquery_data, agg_aliases)
                        
                        # Build semi-join with proper column references
                        if inner_column:
                            # Handle complex expressions in outer_column or inner_column
                            # Use 'col()' wrapper unless it's already a complex expression string
                            
                            # Inner column logic
                            if type(inner_column) is dict:
                                inner_col_str = translate_value(inner_column)
                            else:
                                inner_col_str = f'col("{inner_column}")'
                            
                            # If outer_column is dict, translate it
                            if type(outer_column) is dict:
                                outer_col_str = translate_value(outer_column)
                            else:
                                outer_col_str = f'col("{outer_column}")' if "col(" not in str(outer_column) else str(outer_column)
        
                            v_final_stmt = v_final_stmt + f"\\\n.join(({subquery_pyspark}), {outer_col_str} == {inner_col_str}, 'semi')"
                        else:
                             # Fallback for when inner column is not found (e.g. SELECT *)
                             # Check if subquery_data is simple table/CTE reference
                             # 'select': {'value': 'col'}, 'from': 'cte'
                             if 'select' in subquery_data and type(subquery_data['select']) is dict and 'value' in subquery_data['select']:
                                 inner_col_val = subquery_data['select']['value']
                                 inner_col_str = f'col("{inner_col_val}")'
                                 
                                 if type(outer_column) is dict:
                                     outer_col_str = translate_value(outer_column)
                                 else:
                                     outer_col_str = f'col("{outer_column}")' if "col(" not in str(outer_column) else str(outer_column)
                                     
                                 v_final_stmt = v_final_stmt + f"\\\n.join(({subquery_pyspark}), {outer_col_str} == {inner_col_str}, 'semi')"
                             else:
                                 v_final_stmt = v_final_stmt + f"\\\n.filter(\"{outer_column} IN ({format(subquery_data)})\")"

                # 2. Handle regular filters
                if v_fn_where['filters']:
                    filters = v_fn_where['filters']
                    if "col(" in str(filters):
                        v_final_stmt = v_final_stmt + "\\\n.filter("+filters+")"
                    else:
                        v_final_stmt = v_final_stmt + "\\\n.filter(\""+filters+"\")"
            
            # Legacy fallback (if fn_where somehow returns old format)
            elif type(v_fn_where) is dict and v_fn_where.get('type') == 'in_subquery':
                # Original logic for single IN subquery (retained for safety)
                outer_column = v_fn_where['column']
                subquery_data = v_fn_where['subquery']
                inner_column = None
                if 'select' in subquery_data:
                    select_data = subquery_data['select']
                    if type(select_data) is dict:
                        if 'value' in select_data:
                            inner_column = select_data['value']
                    elif type(select_data) is list and len(select_data) > 0:
                        if type(select_data[0]) is dict and 'value' in select_data[0]:
                            inner_column = select_data[0]['value']
                subquery_pyspark = fn_genSQL(subquery_data, agg_aliases)
                if inner_column:
                    inner_col_str = f'col("{inner_column}")'
                    outer_col_str = f'col("{outer_column}")' if "col(" not in str(outer_column) else str(outer_column)
                    v_final_stmt = v_final_stmt + f"\\\n.join(({subquery_pyspark}), {outer_col_str} == {inner_col_str}, 'semi')"
                else:
                    v_final_stmt = v_final_stmt + f"\\\n.filter(\"{outer_column} IN ({format(subquery_data)})\")"

            else:
                # Regular filter string
                if "col(" in str(v_fn_where):
                    v_final_stmt = v_final_stmt + "\\\n.filter("+v_fn_where+")"
                else:
                    v_final_stmt = v_final_stmt + "\\\n.filter(\""+v_fn_where+"\")"
        
        if v_fn_groupby:
            v_final_stmt = v_final_stmt + "\\\n.groupBy("+v_fn_groupby+")"
        if v_fn_agg:
            v_final_stmt = v_final_stmt + "\\\n.agg("+v_fn_agg+")"
            
        # Add scalar subquery joins for HAVING clause (AFTER AGGREGATION)
        # This attaches the scalar value to the grouped results
        v_final_stmt = append_scalar_joins(v_final_stmt, scalar_subqueries_having)
            
        if v_fn_having:
            # Check if HAVING is already PySpark syntax or SQL string
            if "col(" in v_fn_having:
                v_final_stmt = v_final_stmt + "\\\n.filter("+v_fn_having+")"
            else:
                v_final_stmt = v_final_stmt + "\\\n.filter(\""+v_fn_having+"\")"
        if v_fn_select:
            v_final_stmt = v_final_stmt + "\\\n.select("+v_fn_select+")"
        if v_fn_distinct:
            v_final_stmt = v_final_stmt + "\\\n.distinct()"
        if v_fn_orderby:
            v_final_stmt = v_final_stmt + "\\\n.orderBy("+v_fn_orderby+")"
        if v_fn_limit:
            v_final_stmt = v_final_stmt + "\\\n.limit("+str(v_fn_limit)+")"
        
        return v_final_stmt
        
    # --------------------
    # Set operation support
    # --------------------
    def _find_set_op_key(obj):
        if type(obj) is not dict:
            return None
        for k in [
            'union', 'union all', 'union_all',
            'intersect', 'intersect all', 'intersect_all',
            'except', 'except all', 'except_all'
        ]:
            if k in obj:
                return k
        return None

    def fn_genSQL_or_set(obj, agg_aliases=None):
        """Generate PySpark for either a regular SELECT dict or a set operation dict."""
        key = _find_set_op_key(obj)
        if not key:
            return fn_genSQL(obj, agg_aliases)

        parts = obj[key]
        if type(parts) is not list:
            parts = [parts]
        # Translate each part recursively (parts can themselves be set ops)
        translated_parts = [f"({fn_genSQL_or_set(p)})" for p in parts]

        # Fold left using the appropriate operator
        def _fold(op_name, items):
            if not items:
                return ""
            acc = items[0]
            for nxt in items[1:]:
                if op_name in ('union all', 'union_all'):
                    acc = f"{acc}.union({nxt})"
                elif op_name == 'union':
                    acc = f"{acc}.union({nxt}).distinct()"
                elif op_name in ('intersect all', 'intersect_all'):
                    acc = f"{acc}.intersectAll({nxt})"
                elif op_name == 'intersect':
                    acc = f"{acc}.intersect({nxt})"
                elif op_name in ('except all', 'except_all'):
                    acc = f"{acc}.exceptAll({nxt})"
                elif op_name == 'except':
                    # Use subtract for EXCEPT (distinct semantics)
                    acc = f"{acc}.subtract({nxt})"
                else:
                    # Fallback to union if somehow unknown
                    acc = f"{acc}.union({nxt})"
            return acc

        # Normalize key to a canonical form for dispatch
        norm_key = key.replace('_', ' ').lower()
        return _fold(norm_key, translated_parts)

    agg_aliases = {}
    # Handle CTEs (WITH clause) at the top level
    cte_code = ""
    # Check for WITH in list form (if parser returned list of 1)
    if type(v_json) is list:
         # Check if any element has 'with'
         for item in v_json:
             if type(item) is dict and "with" in item:
                 cte_block = item.pop("with")
                 # Ensure list
                 if type(cte_block) is not list:
                     cte_block = [cte_block]
                     
                 for cte in cte_block:
                     name = cte.get("name")
                     value = cte.get("value")
                     if name and value:
                         # Recursively translate the CTE query
                         cte_pyspark = fn_genSQL_or_set(value, agg_aliases)
                         cte_code += f"{name} = {cte_pyspark}\n"
    elif "with" in v_json:
        cte_block = v_json.pop("with")
        # Ensure list
        if type(cte_block) is not list:
            cte_block = [cte_block]
            
        for cte in cte_block:
            name = cte.get("name")
            value = cte.get("value")
            if name and value:
                # Recursively translate the CTE query
                cte_pyspark = fn_genSQL_or_set(value)
                cte_code += f"{name} = {cte_pyspark}\n"

    # Translate the main query
    if type(v_json) is list and len(v_json) == 1:
        v_json = v_json[0]
        
    main_query = fn_genSQL_or_set(v_json)
    return cte_code + main_query

import sys
if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     # # print("Usage: python translator.py input.file")
    queries_from_file = False
    if len(sys.argv) >= 2:
        queries_from_file = True
        queries = []
        file_names = []
        for i in range(1, len(sys.argv)):
            input_file = sys.argv[i]
            with open(input_file, 'r') as file:
                file_names.append(input_file.split('/')[-1])
                content = file.read()
            first_with = content.lower().find("with")
            first_select = content.lower().find("select")
            if first_with == -1 and first_select == -1:
                raise ValueError("No with or select found in the file")
            if first_with != -1 and first_select != -1:
                if first_with < first_select:
                    queries.append(content[first_with:])
                else:
                    queries.append(content[first_select:])
            else:
                if first_with != -1:
                    queries.append(content[first_with:])
                else:
                    queries.append(content[first_select:])
    else:
        queries = [
        # "SELECT * FROM employees",
        # "SELECT id, name FROM customers",
        # "SELECT id FROM users WHERE age > 30",
        # "SELECT * FROM sales WHERE region = 'US'",
        # "SELECT * FROM table WHERE salary BETWEEN 50000 AND 100000",
        # "SELECT * FROM products WHERE category IS NULL",
        # "SELECT * FROM products WHERE category IS NOT NULL",
        # "SELECT UPPER(name), LOWER(city) FROM customers",
        # "SELECT DISTINCT country FROM customers",
        # "SELECT name FROM employees ORDER BY salary DESC",
        # "SELECT name, department, AVG(salary) FROM employees GROUP BY department",
        # "SELECT AVG(salary) FROM employees GROUP BY department",
        # "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5",
        # "SELECT department, SUM(salary) FROM employees GROUP BY department HAVING SUM(salary) > (SELECT AVG(sum_sal) FROM (SELECT SUM(salary) as sum_sal FROM employees GROUP BY department) t)",
        # "SELECT * FROM sales LIMIT 10",
        # "SELECT SUBSTRING(name, 1, 3) FROM users",
        # "SELECT CONCAT(first_name, ' ', last_name) FROM employees",
        # "SELECT LENGTH(description) FROM products",
        # "SELECT DATE_ADD(order_date, 7) FROM orders",
        # "SELECT YEAR(birthdate), MONTH(birthdate), DAY(birthdate) FROM users",
        # "SELECT CASE WHEN salary > 100000 THEN 'High' ELSE 'Low' END FROM employees",
        # "SELECT COALESCE(phone, 'N/A') FROM contacts",
        # "SELECT NULLIF(status, 'inactive') FROM users",
        # "SELECT STR_TO_DATE('01,5,2020', '%d,%m,%Y') FROM dates",
        # "SELECT name FROM employees WHERE department_id IN (SELECT id FROM departments WHERE region = 'West')",
        # "SELECT * FROM (SELECT id, name FROM users WHERE active = 1) AS active_users",
        # "SELECT department, (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS emp_count FROM departments d",
        # "SELECT id FROM table1 UNION SELECT id FROM table2",
        # "SELECT name FROM table1 INTERSECT SELECT name FROM table2",
        # "SELECT name FROM table1 EXCEPT SELECT name FROM table2",
        # "SELECT name, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) FROM employees",
        # "SELECT id, SUM(sales) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM transactions",
        # "SELECT SUM(sales) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM transactions",
        # "SELECT employee_name, salary FROM employees ORDER BY (salary - AVG(salary) OVER (PARTITION BY department)) DESC",
        # """
        # with customer_total_return as
        # (select sr_customer_sk as ctr_customer_sk
        # ,sr_store_sk as ctr_store_sk
        # ,sum(SR_FEE) as ctr_total_return
        # from store_returns
        # ,date_dim
        # where sr_returned_date_sk = d_date_sk
        # and d_year =2000
        # group by sr_customer_sk
        # ,sr_store_sk)
        #  select  c_customer_id
        # from customer_total_return ctr1
        # ,store
        # ,customer
        # where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
        # from customer_total_return ctr2
        # where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
        # and s_store_sk = ctr1.ctr_store_sk
        # and s_state = 'TN'
        # and ctr1.ctr_customer_sk = c_customer_sk
        # order by c_customer_id
        # LIMIT 100;
        # """,
        # """
        # with wscs as
        #  (select sold_date_sk
        #         ,sales_price
        #   from (select ws_sold_date_sk sold_date_sk
        #               ,ws_ext_sales_price sales_price
        #         from web_sales 
        #         union all
        #         select cs_sold_date_sk sold_date_sk
        #               ,cs_ext_sales_price sales_price
        #         from catalog_sales)),
        #  wswscs as 
        #  (select d_week_seq,
        #         sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        #         sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        #         sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        #         sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        #         sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        #         sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        #         sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
        #  from wscs
        #      ,date_dim
        #  where d_date_sk = sold_date_sk
        #  group by d_week_seq)
        #  select d_week_seq1
        #        ,round(sun_sales1/sun_sales2,2)
        #        ,round(mon_sales1/mon_sales2,2)
        #        ,round(tue_sales1/tue_sales2,2)
        #        ,round(wed_sales1/wed_sales2,2)
        #        ,round(thu_sales1/thu_sales2,2)
        #        ,round(fri_sales1/fri_sales2,2)
        #        ,round(sat_sales1/sat_sales2,2)
        #  from
        #  (select wswscs.d_week_seq d_week_seq1
        #         ,sun_sales sun_sales1
        #         ,mon_sales mon_sales1
        #         ,tue_sales tue_sales1
        #         ,wed_sales wed_sales1
        #         ,thu_sales thu_sales1
        #         ,fri_sales fri_sales1
        #         ,sat_sales sat_sales1
        #   from wswscs,date_dim 
        #   where date_dim.d_week_seq = wswscs.d_week_seq and
        #         d_year = 2001) y,
        #  (select wswscs.d_week_seq d_week_seq2
        #         ,sun_sales sun_sales2
        #         ,mon_sales mon_sales2
        #         ,tue_sales tue_sales2
        #         ,wed_sales wed_sales2
        #         ,thu_sales thu_sales2
        #         ,fri_sales fri_sales2
        #         ,sat_sales sat_sales2
        #   from wswscs
        #       ,date_dim 
        #   where date_dim.d_week_seq = wswscs.d_week_seq and
        #         d_year = 2001+1) z
        #  where d_week_seq1=d_week_seq2-53
        #  order by d_week_seq1;
        #         """,
            """
select  dt.d_year 
       ,item.i_brand_id brand_id 
       ,item.i_brand brand
       ,sum(ss_ext_sales_price) sum_agg
 from  date_dim dt 
      ,store_sales
      ,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
   and store_sales.ss_item_sk = item.i_item_sk
   and item.i_manufact_id = 436
   and dt.d_moy=12
 group by dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
LIMIT 100;
        """
    ]

    for i, query in enumerate(queries):
        if queries_from_file:
            print(f"Processing file: {file_names[i]}")
        else:
            print(f"Processing query: {i+1}")
        print(query)
        print(translate_sql_to_pyspark(query))
        print("-"*100)
