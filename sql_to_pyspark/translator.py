# copied from https://sqlandhadoop.com/online-sql-to-pyspark-converter/

from mo_sql_parsing import parse
from mo_sql_parsing import format
import json

def translate_sql_to_pyspark(query: str) -> str:

    v_parse = parse(query)
    v_json = json.loads(json.dumps(v_parse,indent=4))

    # print(f'v_json: {v_json}')
    
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
            # print(f'{select_item} is a window function')
            return True
        # Look inside value
        val = select_item.get('value')
        if type(val) is dict:
            if 'over' in val:
                # print(f'{select_item} is a window function (value)')
                return True
            # function form: {'sum': {'over': {...}, 'value': 'col'}}
            try:
                inner_key = list(val.keys())[0]
                inner_val = val[inner_key]
                if type(inner_val) is dict and 'over' in inner_val:
                    # print(f'{select_item} is a window function (inner_val)')
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
        if 'select' in query_dict:
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
    
    def translate_scalar_subquery_join(subquery_dict, alias_name, outer_table_info):
        """
        Translate a scalar subquery to PySpark using JOIN method.
        Returns a dict with aggregation DataFrame code and join information.
        """
        # print(f'translating scalar subquery join: {subquery_dict}')
        # Extract the aggregate function from SELECT
        select_data = subquery_dict.get('select')
        if not select_data:
            return None
        
        # Get the aggregate function
        agg_func = None
        agg_column = None
        
        if type(select_data) is dict and 'value' in select_data:
            value = select_data['value']
            if type(value) is dict and is_aggregate_function(value):
                # Get the function name and column
                func_name = list(value.keys())[0]
                agg_column = value[func_name]
                agg_func = func_name.lower()
        
        if not agg_func:
            return None
        
        # Get the FROM table
        from_table = subquery_dict.get('from')
        if type(from_table) is dict and 'value' in from_table:
            inner_table = from_table['value']
            inner_alias = from_table.get('name', 'subq')
        elif type(from_table) is str:
            inner_table = from_table
            inner_alias = 'subq'
        else:
            return None
        
        # Get WHERE clause to find correlation
        where_clause = subquery_dict.get('where')
        correlation_column = None
        other_filters = []
        
        if where_clause and type(where_clause) is dict:
            # Check for simple equality correlation
            if 'eq' in where_clause:
                operands = where_clause['eq']
                if type(operands) is list and len(operands) == 2:
                    left, right = operands[0], operands[1]
                    # Find which one is the correlation
                    if type(left) is str and type(right) is str:
                        if '.' in left and '.' in right:
                            # Both qualified - this is the correlation
                            # Parse to find inner table column
                            left_parts = left.split('.')
                            right_parts = right.split('.')
                            
                            # Determine which is inner, which is outer
                            if left_parts[0] == inner_alias or left_parts[0] == inner_table:
                                correlation_column = left_parts[1] if len(left_parts) > 1 else left
                                outer_column = right_parts[1] if len(right_parts) > 1 else right
                            else:
                                correlation_column = right_parts[1] if len(right_parts) > 1 else right
                                outer_column = left_parts[1] if len(left_parts) > 1 else left
            
            # Handle AND conditions with multiple filters
            elif 'and' in where_clause:
                conditions = where_clause['and']
                if type(conditions) is list:
                    for cond in conditions:
                        if type(cond) is dict and 'eq' in cond:
                            operands = cond['eq']
                            if type(operands) is list and len(operands) == 2:
                                left, right = operands[0], operands[1]
                                if '.' in str(left) and '.' in str(right):
                                    # This is likely the correlation
                                    left_parts = str(left).split('.')
                                    right_parts = str(right).split('.')
                                    if left_parts[0] == inner_alias:
                                        correlation_column = left_parts[1] if len(left_parts) > 1 else left
                                        outer_column = right_parts[1] if len(right_parts) > 1 else right
                                    else:
                                        correlation_column = right_parts[1] if len(right_parts) > 1 else right
                                        outer_column = left_parts[1] if len(left_parts) > 1 else left
                                else:
                                    # Other filter condition
                                    other_filters.append(cond)
                        else:
                            other_filters.append(cond)
        
        # Build the aggregation code
        result = {
            'inner_table': inner_table,
            'inner_alias': inner_alias,
            'agg_func': agg_func,
            'agg_column': agg_column,
            'correlation_column': correlation_column,
            'outer_column': outer_column if correlation_column else None,
            'alias_name': alias_name,
            'other_filters': other_filters
        }
        
        return result
    
    def translate_condition(cond_dict):
        """Translate a condition (like 'gt', 'lt', 'eq') to PySpark syntax"""
        # print(f'translating condition: {cond_dict}')
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
            if type(val) is dict and 'literal' in val:
                lit_val = val['literal']
                if type(lit_val) is str:
                    return f"'" + lit_val.replace("'", "\\'") + "'"
                else:
                    return str(lit_val)
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
            if type(in_val) is list and len(in_val) == 2 and type(in_val[1]) is list:
                column, values = in_val[0], in_val[1]
                left_str = f'col("{column}")' if type(column) is str else str(column)
                items = ", ".join(to_python_literal(v) for v in values)
                return f"{left_str}.isin([{items}])"
        if 'nin' in cond_dict or 'not in' in cond_dict:
            nin_val = cond_dict.get('nin', cond_dict.get('not in'))
            if type(nin_val) is list and len(nin_val) == 2 and type(nin_val[1]) is list:
                column, values = nin_val[0], nin_val[1]
                left_str = f'col("{column}")' if type(column) is str else str(column)
                items = ", ".join(to_python_literal(v) for v in values)
                return f"~({left_str}.isin([{items}]))"
        
        # LIKE / NOT LIKE -> startswith/endswith/contains when possible
        def translate_like(col_expr, pattern):
            # print(f'translating like: {col_expr}, {pattern}')
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
                        left_str = str(left)
                    
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
                    elif type(right) is dict and 'literal' in right:
                        # Literal value
                        lit_val = right['literal']
                        if type(lit_val) is str:
                            right_str = f"lit('{lit_val}')"
                        else:
                            right_str = f"lit({lit_val})"
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
        # print(f'translating value: {val}')
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
                if ' ' in val or not val.replace('_', '').isalnum():
                    return f"lit('{val}')"
                else:
                    return f'col("{val}")'
        elif type(val) in (int, float):
            return str(val)
        elif type(val) is dict and 'literal' in val:
            lit_val = val['literal']
            if type(lit_val) is str:
                return f"lit('{lit_val}')"
            else:
                return f"lit({lit_val})"
        else:
            return str(val)
    
    def build_window_spec(over_spec):
        """Build a PySpark Window spec string from an OVER() dict.
        Supports keys: partitionby/partition_by, orderby/order_by, rows/range frames.
        """
        # print(f'building window spec: {over_spec}')
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
                elif type(frame_val) is list and len(frame_val) == 2:
                    start_b, end_b = frame_val[0], frame_val[1]

                start_expr = parse_boundary(start_b)
                end_expr = parse_boundary(end_b)
                if start_expr is not None and end_expr is not None:
                    parts[-1] = parts[-1] + f".{api}({start_expr}, {end_expr})"
                break

        return parts[-1]

    def translate_function(func_dict):
        """Translate a function from parsed SQL structure to PySpark syntax"""
        # print(f'translating function: {func_dict}')
        # # print(func_dict)
        if type(func_dict) is not dict:
            return None
        
        # Handle window functions via OVER clause (top-level form: {'over': {...}})
        if 'over' in func_dict:
            # print(f'has top level over: {func_dict}')
            over_spec = func_dict['over']
            # Expect inner expression under 'value' or 'expr'
            inner_expr_spec = None
            if type(over_spec) is dict:
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
        
        # Get the function name and arguments
        func_name = list(func_dict.keys())[0]
        func_args = func_dict[func_name]
        
        # Convert function name to lowercase for PySpark
        # # print(func_name)
        if func_name in SPECIAL_FUNCTION_MAPPING:
            pyspark_func = SPECIAL_FUNCTION_MAPPING[func_name]
        else:
            pyspark_func = func_name.lower()
        
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
            # print(f'has function-as-window form: {func_args}\n base_call: {base_call}')
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
                    if ' ' in arg or len(arg.strip()) == 0 or not arg.replace('_', '').isalnum():
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
            return f'{pyspark_func}({", ".join(arg_strs)})'
        elif type(func_args) is dict:
            # Complex structure - try SQL format
            try:
                return format({"select": func_dict}).replace("SELECT ", "")
            except:
                return str(func_dict)
        else:
            # Fallback
            return f'{pyspark_func}(col("{str(func_args)}"))'


    def fn_from(value):
        # print(f'translating from: {value}')
        result_from=""
        if type(value) is str:
            result_from = format({ "from": value })
            result_from = result_from[5:]
        elif type(value) is dict:
            # Check if this is a subquery in FROM (derived table)
            if "name" in value.keys() and "value" in value.keys() and type(value['value']) is dict:
                # This is a subquery with an alias
                subquery_sql = fn_genSQL_or_set(value['value'])
                result_from = f"({subquery_sql}).alias(\"{value['name']}\")"
            elif "name" in value.keys():
                result_from = result_from + value['value']+".alias(\""+value['name']+"\")"
            else:
                result_from = result_from + value['value']+""
        elif type(value) is list:
            # Handle JOINs - first element is the base table
            base_table = None
            joins = []
            
            # First pass: detect if this is a JOIN scenario
            has_joins = False
            for item_from in value:
                if type(item_from) is dict and any(key in item_from for key in ['inner join', 'left join', 'right join', 'full outer join', 'cross join']):
                    has_joins = True
                    break
            
            for idx, item_from in enumerate(value):
                if type(item_from) is str:
                    # Base table
                    if base_table is None:
                        base_table = item_from
                    else:
                        # Multiple string items (shouldn't happen in normal cases)
                        base_table += "," + item_from
                elif type(item_from) is dict:
                    # Check if this is a JOIN
                    join_type = None
                    join_table = None
                    join_condition = None
                    
                    # Detect join type
                    if 'inner join' in item_from:
                        join_type = 'inner'
                        join_table = item_from['inner join']
                    elif 'left join' in item_from:
                        join_type = 'left'
                        join_table = item_from['left join']
                    elif 'right join' in item_from:
                        join_type = 'right'
                        join_table = item_from['right join']
                    elif 'full outer join' in item_from:
                        join_type = 'outer'
                        join_table = item_from['full outer join']
                    elif 'cross join' in item_from:
                        join_type = 'cross'
                        join_table = item_from['cross join']
                    
                    if join_type and join_table:
                        # Get ON condition
                        if 'on' in item_from:
                            join_condition = translate_condition(item_from['on'])
                        
                        # Handle join table with alias
                        if type(join_table) is dict and "name" in join_table:
                            join_table_str = join_table['value'] + ".alias(\"" + join_table['name'] + "\")"
                        elif type(join_table) is str:
                            join_table_str = join_table
                        else:
                            join_table_str = str(join_table)
                        
                        joins.append({
                            'type': join_type,
                            'table': join_table_str,
                            'condition': join_condition
                        })
                    elif has_joins and idx == 0 and base_table is None:
                        # First item in a JOIN scenario - this is the base table
                        if "name" in item_from.keys():
                            base_table = item_from['value'] + ".alias(\"" + item_from['name'] + "\")"
                        elif "value" in item_from.keys():
                            base_table = item_from['value']
                    elif not has_joins:
                        # Old logic for non-JOIN scenarios (comma-separated tables)
                        if "name" in item_from.keys():
                            result_from = result_from + item_from['value']+".alias(\""+item_from['name']+"\"),"
                        elif "value" in item_from.keys():
                            result_from = result_from + item_from['value']+","
            
            # Build the result
            if base_table and joins:
                result_from = base_table
                for join in joins:
                    if join['condition']:
                        result_from += f"\\\n.join({join['table']}, {join['condition']}, '{join['type']}')"
                    else:
                        # CROSS JOIN doesn't need condition
                        result_from += f"\\\n.crossJoin({join['table']})"
            elif base_table:
                result_from = base_table
            # else: result_from already set by old logic (comma-separated)
                
        return result_from
            

    def fn_select(value, outer_alias=None, scalar_subq_list=None):
        # print(f'translating select: {value} \n')
        result_select=""
        
        # Check if we should qualify columns (only if outer_alias exists and we have scalar subqueries)
        has_outer_alias = outer_alias is not None
        has_scalar_subqueries = scalar_subq_list is not None and len(scalar_subq_list) > 0
        
        if type(value) is str:
            # Simple column - qualify if we have outer alias and scalar subqueries
            if has_outer_alias and has_scalar_subqueries:
                result_select = result_select + f'col("{outer_alias}.{value}"),'
            else:
                result_select = result_select + "\""+value+"\","
        elif type(value) is dict:
            # Handle SELECT * case
            if "all_columns" in value.keys():
                result_select = "\"*\""
            elif "name" in value.keys():
                # Check if it's an aggregate function
                if type(value['value']) is dict and is_aggregate_function(value['value']):
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
                    func_str = translate_function(value['value'])
                    result_select = result_select + func_str + ".alias(\""+value['name']+"\"),"
                else:
                    # Regular column with alias - qualify if we have outer alias and scalar subqueries
                    if has_outer_alias and has_scalar_subqueries and not '.' in str(value['value']):
                        result_select = result_select + f'col("{outer_alias}.{value["value"]}").alias("{value["name"]}"),'
                    else:
                        result_select = result_select + "\""+value['value']+"\".alias(\""+value['name']+"\"),"
            elif "value" in value.keys() and type(value['value']) is dict:
                if is_aggregate_function(value['value']):
                    # Aggregate function - skip, handled by fn_agg
                    pass
                elif 'select' in value['value']:
                    # This is a scalar subquery - not directly supported in PySpark
                    # Would need to be converted to a join or window function
                    result_select = result_select + "\"# SUBQUERY\","
                else:
                    # Scalar function without alias
                    func_str = translate_function(value['value'])
                    # Handle parent-level OVER on this select item
                    if 'over' in value:
                        func_str = func_str + f".over({build_window_spec(value['over'])})"
                    result_select = result_select + func_str + ","
            else:
                result_select = result_select + "\""+value['value']+"\""
        elif type(value) is list:
            for item_select in value:
                if type(item_select) is dict:
                    if type(item_select['value']) is dict:
                        if is_aggregate_function(item_select['value']):
                            # Aggregate function
                            if "name" in item_select.keys():
                                result_select = result_select + "\""+item_select['name']+"\","
                            else:
                                # Skip - handled by fn_agg
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
                        else:
                            # Scalar function
                            func_str = translate_function(item_select['value'])
                            if "name" in item_select.keys():
                                result_select = result_select + func_str + ".alias(\""+item_select['name']+"\"),"
                            else:
                                result_select = result_select + func_str + ","
                    else:
                        # Regular column or function
                        if "name" in item_select.keys():
                            # Column with alias - qualify if we have outer alias and scalar subqueries
                            if has_outer_alias and has_scalar_subqueries and not '.' in str(item_select['value']):
                                # Handle function-with-over at parent level (rare structure)
                                if 'over' in item_select and type(item_select['value']) is dict:
                                    func_str = translate_function(item_select['value'])
                                    func_str = func_str + f".over({build_window_spec(item_select['over'])})"
                                    result_select = result_select + func_str + f'.alias("{item_select["name"]}"),'
                                else:
                                    result_select = result_select + f'col("{outer_alias}.{item_select["value"]}").alias("{item_select["name"]}"),'
                            else:
                                if 'over' in item_select and type(item_select['value']) is dict:
                                    func_str = translate_function(item_select['value'])
                                    func_str = func_str + f".over({build_window_spec(item_select['over'])})"
                                    result_select = result_select + func_str + f'.alias("{item_select["name"]}"),'
                                else:
                                    result_select = result_select + "\""+item_select['value']+"\".alias(\""+item_select['name']+"\"),"
                        else:
                            # Column without alias - qualify if we have outer alias and scalar subqueries
                            if has_outer_alias and has_scalar_subqueries and not '.' in str(item_select['value']):
                                if 'over' in item_select and type(item_select['value']) is dict:
                                    func_str = translate_function(item_select['value'])
                                    func_str = func_str + f".over({build_window_spec(item_select['over'])})"
                                    result_select = result_select + func_str + ","
                                else:
                                    result_select = result_select + f'col("{outer_alias}.{item_select["value"]}"),'
                            else:
                                if 'over' in item_select and type(item_select['value']) is dict:
                                    func_str = translate_function(item_select['value'])
                                    func_str = func_str + f".over({build_window_spec(item_select['over'])})"
                                    result_select = result_select + func_str + ","
                                else:
                                    result_select = result_select + "\""+item_select['value']+"\"," 
        return result_select[:-1] if result_select.endswith(",") else result_select

    def fn_where(value):
        """
        Translate WHERE clause to PySpark.
        Handles simple conditions and IN subqueries.
        """
        # print(f'translating where: {value}')
        result_where=""
        
        # Check if this is an IN clause with a subquery
        if type(value) is dict and 'in' in value:
            in_clause = value['in']
            if type(in_clause) is list and len(in_clause) == 2:
                column = in_clause[0]
                subquery_or_list = in_clause[1]
                
                # Check if the second element is a subquery (dict with 'select')
                if type(subquery_or_list) is dict and 'select' in subquery_or_list:
                    # This is a subquery - we need to handle it specially
                    # Return special marker that will be handled in fn_genSQL
                    return {
                        'type': 'in_subquery',
                        'column': column,
                        'subquery': subquery_or_list
                    }
        
        # Try to translate to PySpark column expressions
        if type(value) is dict:
            translated = translate_condition(value)
            if translated and translated != str(value):
                return translated
        
        # Default: use SQL format for the WHERE clause
        result_where = format({ "where": value })[6:]
        return result_where


    def fn_groupby(value):
        # print(f'translating groupby: {value}')
        result_groupby=""
        result_groupby = format({ "groupby": value })[9:]
        return result_groupby
    
    def fn_having(value, agg_aliases=None):
        """
        Translate HAVING clause to PySpark, replacing aggregate functions with their aliases
        and converting to PySpark column expressions.
        """
        # print(f'translating having: {value}')
        def replace_agg_in_structure(obj):
            """Recursively replace aggregate functions with column references"""
            if type(obj) is dict:
                # Check if this is an aggregate function
                if is_aggregate_function(obj):
                    # Get the function name and argument
                    func_name = list(obj.keys())[0]
                    func_arg = obj[func_name]
                    agg_expr = f"{func_name.upper()}({func_arg})"
                    
                    # If we have an alias for this aggregate, return a column reference
                    if agg_aliases and agg_expr in agg_aliases:
                        return agg_aliases[agg_expr]
                    # Otherwise, return as-is (will be handled by translate_condition)
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

    def fn_agg(query):
        # print(f'translating agg: {query}')
        v_parse = parse(query)
        v_agg = ""
        agg_aliases = {}  # Map from aggregate expression to alias
        
        # Handle both single (dict) and multiple (list) select items
        select_items = v_parse["select"]
        if type(select_items) is dict:
            select_items = [select_items]
        
        for i in select_items:
            if type(i["value"]) is dict and is_aggregate_function(i["value"]) and not is_window_expression(i):
                # Only process actual aggregate functions
                for key,value in i["value"].items():
                    # Only add alias if SQL has AS clause
                    if "name" in i:
                        alias_name = i["name"]
                        v_agg = v_agg + (key+"("+"col(\""+str(value)+"\")"+").alias('"+alias_name+"')") +","
                    else:
                        # No alias - create automatic alias
                        # Format: function_name_column (e.g., count_star, sum_salary)
                        col_name = str(value).replace("*", "star").replace(".", "_")
                        alias_name = f"{key.lower()}_{col_name}"
                        v_agg = v_agg + (key+"("+"col(\""+str(value)+"\")"+").alias('"+alias_name+"')") +","
                    
                    # Store mapping for HAVING clause translation
                    agg_expr = f"{key.upper()}({value})"
                    agg_aliases[agg_expr] = alias_name
        
        v_agg = v_agg.replace("\n", "")
        return v_agg[:-1] if v_agg else "", agg_aliases


    def fn_orderby(query):
        # print(f'translating orderby: {query}')
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
            v_orderby_collist = v_orderby_collist + "col(\""+str(i.get("value", ""))+"\")." +v_sortorder+","
        return v_orderby_collist[:-1]


    def fn_limit(query):
        # print(f'translating limit: {query}')
        v_parse = parse(query)
        v_limit = v_parse["limit"]
        return v_limit


    def fn_genSQL(data):
        v_fn_from = v_fn_where = v_fn_groupby = v_fn_agg = v_fn_select = v_fn_orderby = v_fn_limit = v_fn_having = ""
        v_fn_distinct = False
        has_aggregate = False
        scalar_subqueries = []  # Store info about scalar subqueries to join
        use_spark_sql_fallback = False
        outer_table_alias = None  # Track the outer table alias for column qualification
        agg_aliases = {}  # Store aggregate function aliases for HAVING clause
        
        # First pass: detect scalar subqueries in SELECT and check nesting depth
        if "select" in data:
            select_value = data["select"]
            select_items = [select_value] if type(select_value) is dict else select_value
            
            for item in select_items:
                if type(item) is dict and "value" in item:
                    value = item["value"]
                    
                    # Check if this is a scalar subquery
                    if type(value) is dict and 'select' in value:
                        # This is a scalar subquery
                        nesting_depth = calculate_nesting_depth(value)
                        alias_name = item.get('name', 'subquery_result')
                        
                        # Check nesting depth for fallback decision
                        if nesting_depth >= 3:
                            # Too complex - use spark.sql fallback
                            use_spark_sql_fallback = True
                            break
                        else:
                            # Try JOIN method
                            subq_info = translate_scalar_subquery_join(value, alias_name, None)
                            if subq_info:
                                scalar_subqueries.append(subq_info)
                            else:
                                # Couldn't translate - mark for fallback
                                use_spark_sql_fallback = True
                                break
                    
                    # Check for regular aggregate functions
                    if is_aggregate_function(value):
                        has_aggregate = True
        
        # If fallback is needed, return spark.sql() wrapper
        if use_spark_sql_fallback:
            sql_query = format(data)
            return f'spark.sql("""{sql_query}""")'
        
        # Extract outer table alias BEFORE processing SELECT
        # (since SELECT may need this info for column qualification)
        if "from" in data:
            from_value = data["from"]
            if type(from_value) is dict and 'name' in from_value:
                outer_table_alias = from_value['name']
            elif type(from_value) is str:
                outer_table_alias = None
        
        # Check if SELECT has aggregate functions (not just any function). Skip window expressions.
        if "select" in data:
            select_value = data["select"]
            if type(select_value) is dict and "value" in select_value:
                has_aggregate = (is_aggregate_function(select_value["value"]) and not is_window_expression(select_value))
            elif type(select_value) is list:
                for item in select_value:
                    if type(item) is dict and "value" in item:
                        if is_aggregate_function(item["value"]) and not is_window_expression(item):
                            has_aggregate = True
                            break
        
        for key,value in data.items():
            # handle from
            if str(key)=="from":
                v_fn_from = fn_from(value)

            #handle where
            if str(key) =="where":
                v_fn_where = fn_where(value)

            #handle groupby
            if str(key) =="groupby":
                v_fn_groupby = fn_groupby(value)

            #handle agg - call if there's a groupby OR if there are aggregate functions (excluding windowed aggs)
            if str(key) =="groupby" or (str(key) == "select" and has_aggregate):
                v_fn_agg, agg_aliases = fn_agg(query)
            
            #handle having
            if str(key) =="having":
                v_fn_having = fn_having(value, agg_aliases)

            #handle select
            if str(key) =="select":
                v_fn_select = fn_select(value, outer_table_alias, scalar_subqueries)
            
            #handle select_distinct
            if str(key) =="select_distinct":
                v_fn_select = fn_select(value, outer_table_alias, scalar_subqueries)
                v_fn_distinct = True

            #handle sort
            if str(key) =="orderby":
                v_fn_orderby = fn_orderby(query)

            #handle limit
            if str(key) =="limit":
                v_fn_limit = fn_limit(query)

        v_final_stmt = ""
        if v_fn_from:
            v_final_stmt = v_final_stmt + v_fn_from
        
        # Add scalar subquery joins (after FROM, before WHERE)
        for subq_info in scalar_subqueries:
            # Build the aggregation DataFrame
            inner_table = subq_info['inner_table']
            inner_alias = subq_info['inner_alias']
            agg_func = subq_info['agg_func']
            agg_column = subq_info['agg_column']
            correlation_column = subq_info['correlation_column']
            outer_column = subq_info['outer_column']
            alias_name = subq_info['alias_name']
            
            # Generate aggregation code
            if agg_column == "*":
                agg_expr = f'{agg_func}(lit(1))'
            else:
                agg_expr = f'{agg_func}(col("{agg_column}"))'
            
            # Build the join
            # Need to create an aggregated temp DataFrame and join it
            agg_alias = f"{alias_name}_agg"
            
            # Add filter for other conditions if needed
            filter_clause = ""
            if subq_info.get('other_filters'):
                # TODO: handle additional filters
                pass
            
            # Build join - using LEFT join to preserve all outer rows
            if correlation_column and outer_column:
                # Format: .join(inner_table.groupBy("correlation_col").agg(...), condition, 'left')
                v_final_stmt += f'\\\n.join({inner_table}.alias("{inner_alias}").groupBy("{correlation_column}").agg({agg_expr}.alias("{alias_name}")).alias("{agg_alias}"), col("{outer_column}") == col("{agg_alias}.{correlation_column}"), "left")'
        
        # Handle WHERE clause - check if it's an IN subquery
        if v_fn_where:
            if type(v_fn_where) is dict and v_fn_where.get('type') == 'in_subquery':
                # Convert IN subquery to semi-join
                outer_column = v_fn_where['column']
                subquery_data = v_fn_where['subquery']
                
                # Extract the inner column from the subquery's SELECT
                inner_column = None
                if 'select' in subquery_data:
                    select_data = subquery_data['select']
                    if type(select_data) is dict and 'value' in select_data:
                        inner_column = select_data['value']
                    elif type(select_data) is list and len(select_data) > 0:
                        if type(select_data[0]) is dict and 'value' in select_data[0]:
                            inner_column = select_data[0]['value']
                
                # Translate the subquery
                subquery_pyspark = fn_genSQL(subquery_data)
                
                # Build semi-join with proper column references
                if inner_column:
                    v_final_stmt = v_final_stmt + f"\\\n.join(({subquery_pyspark}), col(\"{outer_column}\") == col(\"{inner_column}\"), 'semi')"
                else:
                    # Fallback: use SQL IN syntax
                    subquery_sql = format(subquery_data)
                    v_final_stmt = v_final_stmt + f"\\\n.filter(\"{outer_column} IN ({subquery_sql})\")"
            else:
                # Regular filter - check if it's already PySpark syntax or SQL string
                if "col(" in v_fn_where:
                    # Already translated to PySpark column expressions
                    v_final_stmt = v_final_stmt + "\\\n.filter("+v_fn_where+")"
                else:
                    # SQL string syntax
                    v_final_stmt = v_final_stmt + "\\\n.filter(\""+v_fn_where+"\")"
        
        if v_fn_groupby:
            v_final_stmt = v_final_stmt + "\\\n.groupBy(\""+v_fn_groupby+"\")"
        if v_fn_agg:
            v_final_stmt = v_final_stmt + "\\\n.agg("+v_fn_agg+")"
        if v_fn_having:
            # Check if HAVING is already PySpark syntax or SQL string
            if "col(" in v_fn_having:
                # Already translated to PySpark column expressions
                v_final_stmt = v_final_stmt + "\\\n.filter("+v_fn_having+")"
            else:
                # SQL string syntax
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

    def fn_genSQL_or_set(obj):
        """Generate PySpark for either a regular SELECT dict or a set operation dict."""
        key = _find_set_op_key(obj)
        if not key:
            return fn_genSQL(obj)

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

    return fn_genSQL_or_set(v_json)

if __name__ == "__main__":
    # query = """
    # SELECT product_id,
    #     Count(star_rating) as total_rating,
    #     Max(star_rating)   AS best_rating,
    #     Min(star_rating)   AS worst_rating
    # FROM   tbl_books
    # WHERE  verified_purchase = 'Y'
    #     AND review_date BETWEEN '1995-07-22' AND '2015-08-31'
    #     AND marketplace IN ( 'DE', 'US', 'UK', 'FR', 'JP' )
    # GROUP  BY product_id
    # ORDER  BY total_rating asc,product_id desc,best_rating
    # LIMIT  10;
    # """
    querys = [
        "SELECT * FROM employees",
        "SELECT id, name FROM customers",
        "SELECT id FROM users WHERE age > 30",
        "SELECT * FROM sales WHERE region = 'US'",
        "SELECT * FROM table WHERE salary BETWEEN 50000 AND 100000",
        "SELECT * FROM products WHERE category IS NULL",
        "SELECT * FROM products WHERE category IS NOT NULL",
        "SELECT UPPER(name), LOWER(city) FROM customers",
        "SELECT DISTINCT country FROM customers",
        "SELECT name FROM employees ORDER BY salary DESC",
        "SELECT name, department, AVG(salary) FROM employees GROUP BY department",
        "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5",
        "SELECT * FROM sales LIMIT 10",
        "SELECT SUBSTRING(name, 1, 3) FROM users",
        "SELECT CONCAT(first_name, ' ', last_name) FROM employees",
        "SELECT LENGTH(description) FROM products",
        "SELECT DATE_ADD(order_date, 7) FROM orders",
        "SELECT YEAR(birthdate), MONTH(birthdate), DAY(birthdate) FROM users",
        "SELECT CASE WHEN salary > 100000 THEN 'High' ELSE 'Low' END FROM employees",
        "SELECT COALESCE(phone, 'N/A') FROM contacts",
        "SELECT NULLIF(status, 'inactive') FROM users",
        "SELECT STR_TO_DATE('01,5,2020', '%d,%m,%Y') FROM dates",
        "SELECT name FROM employees WHERE department_id IN (SELECT id FROM departments WHERE region = 'West')",
        "SELECT * FROM (SELECT id, name FROM users WHERE active = 1) AS active_users",
        "SELECT department, (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS emp_count FROM departments d",
        "SELECT id FROM table1 UNION SELECT id FROM table2",
        "SELECT name FROM table1 INTERSECT SELECT name FROM table2",
        "SELECT name FROM table1 EXCEPT SELECT name FROM table2",
        "SELECT name, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) FROM employees",
        "SELECT id, SUM(sales) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM transactions",
    ]

    for query in querys:
        print(query)
        print(translate_sql_to_pyspark(query))
        print("-"*100)