#!/usr/bin/env python3
"""
Lightweight TPC-DS Data Generator (without Spark)

Generates sample CSV files for TPC-DS tables that can be loaded into Spark.
This avoids Java version compatibility issues.

Usage:
    python generate_tpcds_data_csv.py
    
    Then load in Spark:
    df = spark.read.csv("tpcds_data/store_sales.csv", header=True)
"""

import csv
import os
from pathlib import Path
from datetime import datetime, timedelta
import random

# Configuration
DATA_DIR = "tpcds_data"
NUM_YEARS = 2
NUM_STORES = 50
NUM_CUSTOMERS = 500
NUM_ITEMS = 1000
NUM_DATES = 365 * NUM_YEARS
NUM_SALES = 10000
NUM_INVENTORY = 5000

# Random seeds for reproducibility
random.seed(42)


def create_output_dir():
    """Create output directory."""
    Path(DATA_DIR).mkdir(exist_ok=True)
    print(f"Using output directory: {DATA_DIR}/")


def generate_date_dim():
    """Generate date_dim table."""
    print("Generating date_dim...")
    
    output_file = os.path.join(DATA_DIR, "date_dim.csv")
    base_date = datetime(2020, 1, 1)
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow([
            'd_date_sk', 'd_date_id', 'd_date', 'd_month_seq', 'd_week_seq', 
            'd_quarter_seq', 'd_year', 'd_dow', 'd_moy', 'd_dom', 'd_qoy',
            'd_fy_year', 'd_fy_quarter_seq', 'd_fy_week_seq', 'd_day_name',
            'd_quarter_name', 'd_holiday', 'd_weekend'
        ])
        
        # Write data
        for i in range(NUM_DATES):
            current_date = base_date + timedelta(days=i)
            writer.writerow([
                i + 1,
                f"D{current_date.strftime('%Y%m%d')}",
                current_date.strftime('%Y-%m-%d'),
                (current_date.year - 2020) * 12 + current_date.month,
                int(current_date.strftime("%W")),
                (current_date.year - 2020) * 4 + (current_date.month - 1) // 3 + 1,
                current_date.year,
                current_date.weekday(),
                current_date.month,
                current_date.day,
                (current_date.month - 1) // 3 + 1,
                current_date.year,
                ((current_date.month - 1) // 3 + 1) + (current_date.year - 2020) * 4,
                int(current_date.strftime("%W")),
                current_date.strftime("%A"),
                {0: "Q1", 1: "Q2", 2: "Q3", 3: "Q4"}[(current_date.month - 1) // 3],
                "Y" if current_date.weekday() >= 5 else "N",
                "Y" if current_date.weekday() >= 5 else "N",
            ])
    
    print(f"  ✓ Generated {NUM_DATES} date records")


def generate_customer():
    """Generate customer table."""
    print("Generating customer...")
    
    output_file = os.path.join(DATA_DIR, "customer.csv")
    first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Robert', 'Lisa', 'William', 'Mary']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'c_customer_sk', 'c_customer_id', 'c_first_name', 'c_last_name', 'c_email_address'
        ])
        
        for i in range(1, NUM_CUSTOMERS + 1):
            writer.writerow([
                i,
                f"CUST{i:010d}",
                random.choice(first_names),
                random.choice(last_names),
                f"cust{i}@email.com"
            ])
    
    print(f"  ✓ Generated {NUM_CUSTOMERS} customer records")


def generate_item():
    """Generate item table."""
    print("Generating item...")
    
    output_file = os.path.join(DATA_DIR, "item.csv")
    categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Furniture', 'Garden']
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'i_item_sk', 'i_item_id', 'i_item_desc', 'i_category', 'i_current_price', 'i_wholesale_cost'
        ])
        
        for i in range(1, NUM_ITEMS + 1):
            writer.writerow([
                i,
                f"ITEM{i:010d}",
                f"Product Name {i}",
                random.choice(categories),
                round(random.uniform(5, 500), 2),
                round(random.uniform(10, 1000), 2)
            ])
    
    print(f"  ✓ Generated {NUM_ITEMS} item records")


def generate_store():
    """Generate store table."""
    print("Generating store...")
    
    output_file = os.path.join(DATA_DIR, "store.csv")
    states = ['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            's_store_sk', 's_store_id', 's_store_name', 's_state'
        ])
        
        for i in range(1, NUM_STORES + 1):
            writer.writerow([
                i,
                f"STORE{i:010d}",
                f"Store {i}",
                random.choice(states)
            ])
    
    print(f"  ✓ Generated {NUM_STORES} store records")


def generate_store_sales():
    """Generate store_sales table."""
    print("Generating store_sales...")
    
    output_file = os.path.join(DATA_DIR, "store_sales.csv")
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'ss_item_sk', 'ss_store_sk', 'ss_customer_sk', 'ss_sold_date_sk',
            'ss_quantity', 'ss_wholesale_cost', 'ss_list_price', 'ss_sales_price', 'ss_net_profit'
        ])
        
        for i in range(1, NUM_SALES + 1):
            quantity = random.randint(1, 20)
            unit_price = round(random.uniform(5, 100), 2)
            net_profit = round(unit_price * quantity * random.uniform(0.9, 1.0), 2)
            
            writer.writerow([
                random.randint(1, NUM_ITEMS),
                random.randint(1, NUM_STORES),
                random.randint(1, NUM_CUSTOMERS),
                random.randint(1, NUM_DATES),
                quantity,
                unit_price,
                unit_price * random.uniform(1.2, 2.5),
                unit_price * quantity * random.uniform(0.8, 1.0),
                net_profit
            ])
    
    print(f"  ✓ Generated {NUM_SALES} store_sales records")


def generate_inventory():
    """Generate inventory table."""
    print("Generating inventory...")
    
    output_file = os.path.join(DATA_DIR, "inventory.csv")
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'inv_item_sk', 'inv_warehouse_sk', 'inv_date_sk', 'inv_qty_on_hand'
        ])
        
        for i in range(1, NUM_INVENTORY + 1):
            writer.writerow([
                random.randint(1, NUM_ITEMS),
                random.randint(1, NUM_STORES),
                random.randint(1, NUM_DATES),
                random.randint(0, 1000)
            ])
    
    print(f"  ✓ Generated {NUM_INVENTORY} inventory records")


def main():
    print("="*80)
    print("TPC-DS SAMPLE DATA GENERATOR (CSV)")
    print("="*80 + "\n")
    
    create_output_dir()
    
    print("\nGenerating tables...")
    generate_date_dim()
    generate_customer()
    generate_item()
    generate_store()
    generate_store_sales()
    generate_inventory()
    
    print("\n" + "="*80)
    print("SUCCESS: All tables generated as CSV files")
    print("="*80)
    print(f"\nFiles saved to: {DATA_DIR}/")
    print("\nLoad in Spark with:")
    print("  df = spark.read.csv('tpcds_data/store_sales.csv', header=True, inferSchema=True)")
    print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    main()
