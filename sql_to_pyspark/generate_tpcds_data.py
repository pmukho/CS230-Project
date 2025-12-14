#!/usr/bin/env python3
"""
TPC-DS Sample Data Generator for PySpark Testing

Generates realistic sample data for key TPC-DS tables to test the
SQL-to-PySpark translator with production-like schemas and data.

Tables generated:
- date_dim: Date dimension (1000+ date records)
- customer: Customers (500 records)
- customer_address: Customer addresses (600 records)
- customer_demographics: Demographics (100 records)
- item: Products (1000 records)
- store: Stores (50 records)
- store_sales: Transactions (10,000+ records)
- inventory: Inventory levels (5000+ records)

Usage:
    python generate_tpcds_data.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, 
    DecimalType, DateType, TimestampType, BooleanType
)
from datetime import datetime, timedelta
import random


class TPCDSDataGenerator:
    """Generate TPC-DS sample data for testing."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.tables = {}
    
    def generate_date_dim(self, num_years: int = 2) -> None:
        """Generate date_dim table with realistic date sequences."""
        print("Generating date_dim...")
        
        data = []
        base_date = datetime(2020, 1, 1)
        date_sk = 1
        
        for i in range(365 * num_years):
            current_date = base_date + timedelta(days=i)
            
            data.append((
                date_sk,                           # d_date_sk
                f"D{current_date.strftime('%Y%m%d')}", # d_date_id
                current_date.date(),               # d_date
                (current_date.year - 2020) * 12 + current_date.month,  # d_month_seq
                int(current_date.strftime("%W")),  # d_week_seq
                (current_date.year - 2020) * 4 + (current_date.month - 1) // 3 + 1,  # d_quarter_seq
                current_date.year,                 # d_year
                current_date.weekday(),            # d_dow
                current_date.month,                # d_moy
                current_date.day,                  # d_dom
                (current_date.month - 1) // 3 + 1, # d_qoy
                current_date.year,                 # d_fy_year
                ((current_date.month - 1) // 3 + 1) + (current_date.year - 2020) * 4,  # d_fy_quarter_seq
                int(current_date.strftime("%W")),  # d_fy_week_seq
                current_date.strftime("%A"),       # d_day_name
                {0: "Q1", 1: "Q2", 2: "Q3", 3: "Q4"}[(current_date.month - 1) // 3],  # d_quarter_name
                "Y" if current_date.weekday() >= 5 else "N",  # d_holiday (simplified)
                "Y" if current_date.weekday() >= 5 else "N",  # d_weekend
                "N",                               # d_following_holiday
                (current_date.replace(day=1)).day, # d_first_dom
                (current_date.replace(month=current_date.month % 12 + 1, day=1) - timedelta(days=1)).day,  # d_last_dom
                i - 365 if i >= 365 else None,    # d_same_day_ly
                None,                              # d_same_day_lq
            ))
            date_sk += 1
        
        schema = StructType([
            StructField("d_date_sk", IntegerType(), False),
            StructField("d_date_id", StringType(), True),
            StructField("d_date", DateType(), False),
            StructField("d_month_seq", IntegerType(), True),
            StructField("d_week_seq", IntegerType(), True),
            StructField("d_quarter_seq", IntegerType(), True),
            StructField("d_year", IntegerType(), True),
            StructField("d_dow", IntegerType(), True),
            StructField("d_moy", IntegerType(), True),
            StructField("d_dom", IntegerType(), True),
            StructField("d_qoy", IntegerType(), True),
            StructField("d_fy_year", IntegerType(), True),
            StructField("d_fy_quarter_seq", IntegerType(), True),
            StructField("d_fy_week_seq", IntegerType(), True),
            StructField("d_day_name", StringType(), True),
            StructField("d_quarter_name", StringType(), True),
            StructField("d_holiday", StringType(), True),
            StructField("d_weekend", StringType(), True),
            StructField("d_following_holiday", StringType(), True),
            StructField("d_first_dom", IntegerType(), True),
            StructField("d_last_dom", IntegerType(), True),
            StructField("d_same_day_ly", IntegerType(), True),
            StructField("d_same_day_lq", IntegerType(), True),
        ])
        
        self.tables['date_dim'] = self.spark.createDataFrame(data, schema)
    
    def generate_customer_demographics(self, num_records: int = 100) -> None:
        """Generate customer_demographics table."""
        print("Generating customer_demographics...")
        
        genders = ['M', 'F']
        marital_statuses = ['M', 'S', 'W', 'D']
        education_statuses = ['Unknown', 'Primary', 'Secondary', 'Some College', 'Associate', 'Bachelor', 'Master', 'Doctorate']
        credit_ratings = ['Good', 'Poor', 'Fair', 'Excellent']
        
        data = []
        for i in range(1, num_records + 1):
            data.append((
                i,                                      # cd_demo_sk
                random.choice(genders),                 # cd_gender
                random.choice(marital_statuses),        # cd_marital_status
                random.choice(education_statuses),      # cd_education_status
                random.randint(50000, 500000),          # cd_purchase_estimate
                random.choice(credit_ratings),          # cd_credit_rating
                random.randint(0, 5),                   # cd_dep_count
                random.randint(0, 3),                   # cd_dep_employed_count
                random.randint(0, 2),                   # cd_dep_college_count
            ))
        
        schema = StructType([
            StructField("cd_demo_sk", IntegerType(), False),
            StructField("cd_gender", StringType(), True),
            StructField("cd_marital_status", StringType(), True),
            StructField("cd_education_status", StringType(), True),
            StructField("cd_purchase_estimate", IntegerType(), True),
            StructField("cd_credit_rating", StringType(), True),
            StructField("cd_dep_count", IntegerType(), True),
            StructField("cd_dep_employed_count", IntegerType(), True),
            StructField("cd_dep_college_count", IntegerType(), True),
        ])
        
        self.tables['customer_demographics'] = self.spark.createDataFrame(data, schema)
    
    def generate_customer_address(self, num_records: int = 600) -> None:
        """Generate customer_address table."""
        print("Generating customer_address...")
        
        states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA']
        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']
        street_types = ['St', 'Ave', 'Rd', 'Blvd', 'Dr', 'Ln', 'Ct', 'Way', 'Pl', 'Ter']
        countries = ['United States', 'Canada', 'Mexico']
        location_types = ['condo', 'house', 'apartment', 'townhouse']
        
        data = []
        for i in range(1, num_records + 1):
            data.append((
                i,                                      # ca_address_sk
                f"CA{i:010d}",                          # ca_address_id
                str(random.randint(1, 9999)),           # ca_street_number
                f"{random.choice(['Main', 'Oak', 'Elm', 'Pine', 'Maple'])} {random.choice(street_types)}",  # ca_street_name
                random.choice(street_types),            # ca_street_type
                f"{random.randint(100, 999)}" if random.random() > 0.7 else None,  # ca_suite_number
                random.choice(cities),                  # ca_city
                f"County{random.randint(1, 30)}",       # ca_county
                random.choice(states),                  # ca_state
                f"{random.randint(10000, 99999)}",      # ca_zip
                random.choice(countries),               # ca_country
                round(random.uniform(-8, -5), 2),       # ca_gmt_offset
                random.choice(location_types),          # ca_location_type
            ))
        
        schema = StructType([
            StructField("ca_address_sk", IntegerType(), False),
            StructField("ca_address_id", StringType(), False),
            StructField("ca_street_number", StringType(), True),
            StructField("ca_street_name", StringType(), True),
            StructField("ca_street_type", StringType(), True),
            StructField("ca_suite_number", StringType(), True),
            StructField("ca_city", StringType(), True),
            StructField("ca_county", StringType(), True),
            StructField("ca_state", StringType(), True),
            StructField("ca_zip", StringType(), True),
            StructField("ca_country", StringType(), True),
            StructField("ca_gmt_offset", DecimalType(5, 2), True),
            StructField("ca_location_type", StringType(), True),
        ])
        
        self.tables['customer_address'] = self.spark.createDataFrame(data, schema)
    
    def generate_customer(self, num_records: int = 500, num_demographics: int = 100, num_addresses: int = 600) -> None:
        """Generate customer table."""
        print("Generating customer...")
        
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Robert', 'Lisa', 'William', 'Mary']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
        
        data = []
        for i in range(1, num_records + 1):
            data.append((
                i,                                      # c_customer_sk
                f"CUST{i:010d}",                        # c_customer_id
                random.randint(1, num_demographics),    # c_current_cdemo_sk
                random.randint(1, num_addresses),       # c_current_addr_sk
                random.randint(1, num_addresses),       # c_current_hdemo_sk (household demo)
                datetime(2015, 1, 1).date() + timedelta(days=random.randint(0, 1825)),  # c_first_purchase_date
                f"{random.choice(first_names)} {random.choice(last_names)}",  # c_first_name
                random.choice(last_names),              # c_last_name
                f"cust{i}@email.com",                   # c_email_address
            ))
        
        schema = StructType([
            StructField("c_customer_sk", IntegerType(), False),
            StructField("c_customer_id", StringType(), False),
            StructField("c_current_cdemo_sk", IntegerType(), True),
            StructField("c_current_addr_sk", IntegerType(), True),
            StructField("c_current_hdemo_sk", IntegerType(), True),
            StructField("c_first_purchase_date", DateType(), True),
            StructField("c_first_name", StringType(), True),
            StructField("c_last_name", StringType(), True),
            StructField("c_email_address", StringType(), True),
        ])
        
        self.tables['customer'] = self.spark.createDataFrame(data, schema)
    
    def generate_item(self, num_records: int = 1000) -> None:
        """Generate item table."""
        print("Generating item...")
        
        categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Furniture', 'Garden']
        classes = ['A', 'B', 'C']
        manufacturers = [f"Mfg{i}" for i in range(1, 51)]
        
        data = []
        for i in range(1, num_records + 1):
            data.append((
                i,                                      # i_item_sk
                f"ITEM{i:010d}",                        # i_item_id
                f"Product Name {i}",                    # i_item_desc
                random.choice(categories),              # i_category
                random.choice(categories),              # i_category_id
                random.choice(classes),                 # i_class
                random.randint(1, 20),                  # i_class_id
                round(random.uniform(5, 500), 2),       # i_current_price
                round(random.uniform(10, 1000), 2),     # i_wholesale_cost
                random.choice(manufacturers),           # i_manager_id
                1 if random.random() > 0.3 else 0,     # i_product_flag
            ))
        
        schema = StructType([
            StructField("i_item_sk", IntegerType(), False),
            StructField("i_item_id", StringType(), False),
            StructField("i_item_desc", StringType(), True),
            StructField("i_category", StringType(), True),
            StructField("i_category_id", StringType(), True),
            StructField("i_class", StringType(), True),
            StructField("i_class_id", IntegerType(), True),
            StructField("i_current_price", DecimalType(8, 2), True),
            StructField("i_wholesale_cost", DecimalType(8, 2), True),
            StructField("i_manager_id", StringType(), True),
            StructField("i_product_flag", IntegerType(), True),
        ])
        
        self.tables['item'] = self.spark.createDataFrame(data, schema)
    
    def generate_store(self, num_records: int = 50) -> None:
        """Generate store table."""
        print("Generating store...")
        
        states = ['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
        
        data = []
        for i in range(1, num_records + 1):
            data.append((
                i,                                      # s_store_sk
                f"STORE{i:010d}",                       # s_store_id
                f"Store {i}",                           # s_store_name
                f"Manager {i}",                         # s_store_manager
                datetime(2000, 1, 1).date() + timedelta(days=random.randint(0, 7300)),  # s_open_date
                datetime(2030, 12, 31).date(),          # s_close_date
                1,                                      # s_employees
                round(random.uniform(50000, 500000), 2),  # s_floor_space
                random.choice(states),                  # s_state
                random.randint(0, 9),                   # s_county
            ))
        
        schema = StructType([
            StructField("s_store_sk", IntegerType(), False),
            StructField("s_store_id", StringType(), False),
            StructField("s_store_name", StringType(), True),
            StructField("s_store_manager", StringType(), True),
            StructField("s_open_date", DateType(), True),
            StructField("s_close_date", DateType(), True),
            StructField("s_employees", IntegerType(), True),
            StructField("s_floor_space", DecimalType(15, 2), True),
            StructField("s_state", StringType(), True),
            StructField("s_county", IntegerType(), True),
        ])
        
        self.tables['store'] = self.spark.createDataFrame(data, schema)
    
    def generate_store_sales(self, num_records: int = 10000, num_stores: int = 50, num_customers: int = 500, num_items: int = 1000, num_dates: int = 730) -> None:
        """Generate store_sales table (fact table)."""
        print("Generating store_sales...")
        
        statuses = ['Completed', 'Pending', 'Returned', 'Shipped']
        
        data = []
        for i in range(1, num_records + 1):
            quantity = random.randint(1, 20)
            unit_price = round(random.uniform(5, 100), 2)
            
            data.append((
                i,                                      # ss_item_sk
                random.randint(1, num_stores),          # ss_store_sk
                random.randint(1, num_customers),       # ss_customer_sk
                random.randint(1, num_dates),           # ss_sold_date_sk
                random.randint(1, 24),                  # ss_sold_time_sk
                random.randint(1, num_items),           # ss_item_sk (duplicate in schema)
                random.randint(1, 20),                  # ss_promo_sk
                quantity,                               # ss_quantity
                unit_price,                             # ss_wholesale_cost
                unit_price * random.uniform(1.2, 2.5),  # ss_list_price
                unit_price * quantity * random.uniform(0.8, 1.0),  # ss_sales_price
                round(unit_price * quantity * random.uniform(0, 0.1), 2),  # ss_ext_discount_amt
                round(unit_price * quantity, 2),        # ss_ext_sales_price
                round(unit_price * quantity * 0.08, 2), # ss_ext_wholesale_cost
                random.randint(0, 10),                  # ss_ext_list_price
                round(random.uniform(1, 50), 2),        # ss_ext_tax
                round(random.uniform(0, 100), 2),       # ss_coupon_amt
                round(unit_price * quantity * random.uniform(0.9, 1.0), 2),  # ss_net_paid
                round(unit_price * quantity * random.uniform(0.9, 1.0), 2),  # ss_net_paid_inc_tax
                round(unit_price * quantity * random.uniform(0.9, 1.0), 2),  # ss_net_profit
            ))
        
        schema = StructType([
            StructField("ss_item_sk", IntegerType(), False),
            StructField("ss_store_sk", IntegerType(), False),
            StructField("ss_customer_sk", IntegerType(), True),
            StructField("ss_sold_date_sk", IntegerType(), True),
            StructField("ss_sold_time_sk", IntegerType(), True),
            StructField("ss_item_sk_dup", IntegerType(), True),
            StructField("ss_promo_sk", IntegerType(), True),
            StructField("ss_quantity", IntegerType(), True),
            StructField("ss_wholesale_cost", DecimalType(8, 2), True),
            StructField("ss_list_price", DecimalType(8, 2), True),
            StructField("ss_sales_price", DecimalType(8, 2), True),
            StructField("ss_ext_discount_amt", DecimalType(10, 2), True),
            StructField("ss_ext_sales_price", DecimalType(12, 2), True),
            StructField("ss_ext_wholesale_cost", DecimalType(12, 2), True),
            StructField("ss_ext_list_price", IntegerType(), True),
            StructField("ss_ext_tax", DecimalType(10, 2), True),
            StructField("ss_coupon_amt", DecimalType(10, 2), True),
            StructField("ss_net_paid", DecimalType(12, 2), True),
            StructField("ss_net_paid_inc_tax", DecimalType(12, 2), True),
            StructField("ss_net_profit", DecimalType(12, 2), True),
        ])
        
        self.tables['store_sales'] = self.spark.createDataFrame(data, schema)
    
    def generate_inventory(self, num_records: int = 5000, num_stores: int = 50, num_items: int = 1000, num_dates: int = 730) -> None:
        """Generate inventory table."""
        print("Generating inventory...")
        
        data = []
        for i in range(1, num_records + 1):
            data.append((
                random.randint(1, num_items),           # inv_item_sk
                random.randint(1, num_stores),          # inv_warehouse_sk
                random.randint(1, num_dates),           # inv_date_sk
                random.randint(0, 1000),                # inv_qty_on_hand
            ))
        
        schema = StructType([
            StructField("inv_item_sk", IntegerType(), True),
            StructField("inv_warehouse_sk", IntegerType(), True),
            StructField("inv_date_sk", IntegerType(), True),
            StructField("inv_qty_on_hand", IntegerType(), True),
        ])
        
        self.tables['inventory'] = self.spark.createDataFrame(data, schema)
    
    def generate_all(self) -> dict:
        """Generate all tables."""
        self.generate_date_dim(num_years=2)
        self.generate_customer_demographics(num_records=100)
        self.generate_customer_address(num_records=600)
        self.generate_customer(num_records=500)
        self.generate_item(num_records=1000)
        self.generate_store(num_records=50)
        self.generate_store_sales(num_records=10000)
        self.generate_inventory(num_records=5000)
        
        return self.tables
    
    def register_tables(self) -> None:
        """Register all DataFrames as temporary Spark SQL views."""
        for table_name, df in self.tables.items():
            df.createOrReplaceTempView(table_name)
            print(f"Registered: {table_name} ({df.count()} rows)")


def create_spark_session() -> SparkSession:
    """Create a Spark session for TPC-DS testing."""
    return SparkSession.builder \
        .appName("TPCDSTest") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


if __name__ == "__main__":
    print("="*80)
    print("TPC-DS SAMPLE DATA GENERATOR FOR PYSPARK")
    print("="*80)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Generate all tables
    generator = TPCDSDataGenerator(spark)
    tables = generator.generate_all()
    
    # Register as Spark SQL views
    generator.register_tables()
    
    # Show sample data
    print("\n" + "="*80)
    print("SAMPLE DATA FROM KEY TABLES")
    print("="*80)
    
    for table_name in ['date_dim', 'customer', 'store', 'item', 'store_sales']:
        if table_name in tables:
            print(f"\n{table_name}:")
            print("-"*80)
            tables[table_name].show(5)
    
    print("\n" + "="*80)
    print("Tables ready for testing. Try running SQL queries:")
    print("  Example: spark.sql('SELECT * FROM store_sales LIMIT 10').show()")
    print("="*80 + "\n")
    
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("\nShutting down Spark session...")
        spark.stop()
