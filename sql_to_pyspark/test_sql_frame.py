# Instead of: from pyspark.sql import SparkSession
# Use SQLFrame's standalone session
from sqlframe.standalone import StandaloneSession, StandaloneUDFRegistration
from sqlframe.base import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.column import Column
def test_sql_frame():
    # 1. Initialize a dummy session (no cluster needed)
    session = StandaloneSession()

    # 2. Define a schema and create a small in-memory DataFrame
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("second_name", StringType(), True),
            StructField("floor", IntegerType(), True),
        ]
    )

    data = [
        (1, "Alice", "Smith", 3),
        (2, "Bob", "Jones", 5),
    ]

    # df = session.createDataFrame(data, schema=schema)
    # df = session.table("employees")
    # df = session.createDataFrame(schema=schema)
    session.catalog.add_table("test_table", schema)
    df = session.read.table("test_table")

    # 3. Apply standard PySpark-style transformations
    df_filtered = (
        df
        .select(
            F.col("id"),
            F.concat_ws(
                F.lit(" "),
                F.col("first_name"),
                F.col("second_name"),
            ).alias("name"),
            (F.col("floor") * F.lit("steps_perfloor")).alias("steps_to_desk"),
        )
    )

    # 4. Generate the SQL string
    print(df_filtered.sql())

def test_sql_frame_with_UDF():
    session = StandaloneSession()
    # session.register("test_udf", test_udf)
    # df = session.table("test_table")
    schema = StructType(
        [
            StructField("floor", IntegerType(), True),
        ]
    )
    session.catalog.add_table("test_table", schema)
    df = session.read.table("test_table")
    df_filtered = df.filter(test_udf(F.col("floor")) > 5)
    print(df_filtered.sql())

def test_udf(floor):
    return floor * 10

def test_sql_frame_with_UDF2():
    session = StandaloneSession()
    schema = StructType(
        [
            StructField("Seqno", IntegerType(), True),
            StructField("Name", StringType(), True),
        ]
    )
    session.catalog.add_table("test_table", schema)
    udf_registration = StandaloneUDFRegistration(session)
    print(dir(udf_registration))
    # udf_registration.register("convertUDF", convertStr, returnType=StringType())
    df = session.read.table("test_table")
    # convertUDF = F.udf(convertStr, returnType=StringType())
    df_filtered = df.select(F.col("Seqno"), F.call_function("convertUDF", F.col("Name")).alias("Name") )
    print(df_filtered.sql())

def convertUDF(col: Column) -> Column:
    res = []
    for row in col:
        res.append(convertStr(row))
    return Column(res, StringType())


def convertStr(str: str) -> StringType():
    resStr=""
    arr = str.split(" ")   
    for x in arr:
        resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr


if __name__ == "__main__":
    test_sql_frame_with_UDF2()