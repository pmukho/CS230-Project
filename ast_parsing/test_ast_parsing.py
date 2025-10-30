import unittest
from ast_parsing import SparkCallVisitor, try_parse, UDFInfo

class TestASTParsing(unittest.TestCase):

    def test_simple_udf_assignment(self):
        code = "cee = udf(lambda x: x + 1)"
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        tree.visit(visitor)

        self.assertIn("cee", visitor.udfs)
        self.assertFalse(visitor.udfs["cee"].applied_to_df)
        self.assertTrue(visitor.has_udf)

    def test_udf_decorator(self):
        code = """
from pyspark.sql.functions import udf

@udf
def f(x):
    return x + 1
"""
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        tree.visit(visitor)

        self.assertIn("f", visitor.udfs)
        self.assertTrue(visitor.udfs["f"].decorator)

    def test_udf_aliasing(self):
        code = """
my_udf = udf(lambda x: x + 1)
alias_udf = my_udf
"""
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        tree.visit(visitor)

        self.assertIn("my_udf", visitor.udfs)
        self.assertIn("alias_udf", visitor.udfs)
        self.assertIs(visitor.udfs["alias_udf"], visitor.udfs["my_udf"])

    def test_udf_applied_in_df_call(self):
        code = "df.withColumn('new_col', my_udf(df.old_col))"
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        visitor.udfs["my_udf"] = UDFInfo(name="my_udf")
        visitor.known_udfs.add("my_udf")
        tree.visit(visitor)

        self.assertTrue(visitor.udfs["my_udf"].applied_to_df)
        self.assertIn("withColumn", visitor.funcs)

    def test_tuple_unpacking_udf_assignment(self):
        code = "a_udf, b_udf = udf(lambda x: x+1), udf(lambda y: y*2)"
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        tree.visit(visitor)

        self.assertIn("a_udf", visitor.udfs)
        self.assertIn("b_udf", visitor.udfs)
        self.assertTrue(visitor.has_udf)

    def test_third_party_imports(self):
        code = """
import numpy as np
from pandas import DataFrame
"""
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        tree.visit(visitor)

        self.assertIn("numpy", visitor.third_party_libs)
        self.assertIn("pandas", visitor.third_party_libs)

    def test_udf_register_sql(self):
        code = "spark.udf.register('sql_udf_name', my_udf)"
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        visitor.udfs["my_udf"] = UDFInfo(name="my_udf")
        visitor.known_udfs.add("my_udf")
        tree.visit(visitor)

        self.assertTrue(visitor.udfs["my_udf"].registered_sql)

    def test_chained_df_calls(self):
        code = "df.filter(df.age > 18).select('name', 'age').distinct()"
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        tree.visit(visitor)

        self.assertIn("filter", visitor.funcs)
        self.assertIn("select", visitor.funcs)
        self.assertIn("distinct", visitor.funcs)

    def test_nested_udf_calls(self):
        code = "df.withColumn('new', my_udf(other_udf(df.old)))"
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        visitor.udfs["my_udf"] = UDFInfo(name="my_udf")
        visitor.udfs["other_udf"] = UDFInfo(name="other_udf")
        visitor.known_udfs.update(["my_udf", "other_udf"])
        tree.visit(visitor)

        self.assertTrue(visitor.udfs["my_udf"].applied_to_df)
        self.assertTrue(visitor.udfs["other_udf"].applied_to_df)

    def test_udf_decorator_applied(self):
        code = """
@udf
def add_one(x):
    return x + 1

df.withColumn('new', add_one(df.old))
"""
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        tree.visit(visitor)

        self.assertIn("add_one", visitor.udfs)
        self.assertTrue(visitor.udfs["add_one"].decorator)
        self.assertTrue(visitor.udfs["add_one"].applied_to_df)

    def test_multiple_udfs_in_args(self):
        code = "df.withColumn('col', aaa(bbb(df.old)))"
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        visitor.udfs["aaa"] = UDFInfo(name="aaa")
        visitor.udfs["bbb"] = UDFInfo(name="bbb")
        visitor.known_udfs.update(["aaa", "bbb"])
        tree.visit(visitor)

        self.assertTrue(visitor.udfs["bbb"].applied_to_df)
        self.assertTrue(visitor.udfs["bbb"].applied_to_df)

    def test_udf_in_callback(self):
        code = "df.filter(my_udf(df.age) > 10)"
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        visitor.udfs["my_udf"] = UDFInfo(name="my_udf")
        visitor.known_udfs.add("my_udf")
        tree.visit(visitor)

        self.assertTrue(visitor.udfs["my_udf"].applied_to_df)
        self.assertIn("filter", visitor.funcs)

    def test_import_aliasing(self):
        code = "import numpy as np\nfrom pandas import DataFrame as DF"
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        tree.visit(visitor)

        self.assertIn("numpy", visitor.third_party_libs)
        self.assertIn("pandas", visitor.third_party_libs)

    def test_select_expr_call(self):
        code = "df.selectExpr('my_udf(col1) as col2')"
        tree = try_parse(code)
        visitor = SparkCallVisitor()
        tree.visit(visitor)

        self.assertIn("selectExpr", visitor.funcs)


if __name__ == "__main__":
    unittest.main()