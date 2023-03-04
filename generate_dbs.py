import sqlite3
import random
import shutil
from typing import Literal
from pathlib import Path
import csv
import os

"""
Creates the three files A3Small.db, A3Medium.db, and A3Large.db by randomly
populating them with data as described in assignment 3.
"""

table_sizes = {
    "Small": {
        "Customers": 10_000,
        "Sellers": 500,
        "Orders": 10_000,
        "Order_items": 2_000,
    },
    "Medium": {
        "Customers": 20_000,
        "Sellers": 750,
        "Orders": 20_000,
        "Order_items": 4_000,
    },
    "Large": {
        "Customers": 33_000,
        "Sellers": 1_000,
        "Orders": 33_000,
        "Order_items": 10_000,
    },
}

source_files = {
    "Customers": "olist_customers_dataset.csv",
    "Sellers": "olist_sellers_dataset.csv",
    "Orders": "olist_orders_dataset.csv",
    "Order_items": "olist_order_items_dataset.csv",
}

column_names = {
    "Customers": [
        ("customer_id", "customer_id"),
        ("customer_postal_code", "customer_zip_code_prefix"),
    ],
    "Sellers": [
        ("seller_id", "seller_id"),
        ("seller_postal_code", "seller_zip_code_prefix"),
    ],
    "Orders": [
        ("order_id", "order_id"),
        ("customer_id", "customer_id"),
    ],
    "Order_items": [
        ("order_id", "order_id"),
        ("order_item_id", "order_item_id"),
        ("product_id", "product_id"),
        ("seller_id", "seller_id"),
    ],
}


def n_random_csv_lines(filename: str, n: int) -> list[str]:
    """
    Returns a list of n random lines from the file given by filename.
    """
    with open(Path("csvs") / filename, "r") as f:
        values = list(csv.DictReader(f))
        random.shuffle(values)
        return values[:n]


def add_data(conn: sqlite3.Connection, size: Literal["Small", "Medium", "Large"]):
    """
    Given a database connection and a size ('Small', 'Medium', or 'Large'),
    add the random data to the table using the sizes in table_sizes.
    """
    c = conn.cursor()
    sizes = table_sizes[size]
    for table, filename in source_files.items():
        old_values = n_random_csv_lines(filename, sizes[table])
        col_names = column_names[table]
        values = []

        for val in old_values:
            tmp = {}
            for dbname, csvname in col_names:
                tmp[dbname] = val[csvname]
            values.append(tmp)

        for value in values:
            c.execute(
                f"""
                INSERT INTO "{table}" {tuple(value.keys())}
                    VALUES ({('?,' * len(value.keys()))[:-1]});
                """,
                tuple(value.values()),
            )
    conn.commit()


def main():
    if Path("A3Small.db").exists():
        os.remove("A3Small.db")
    if Path("A3Medium.db").exists():
        os.remove("A3Medium.db")
    if Path("A3Large.db").exists():
        os.remove("A3Large.db")

    small = sqlite3.connect("A3Small.db")
    small_cursor = small.cursor()
    small_cursor.executescript(
        """
	-- olist_customers_dataset.csv
	CREATE TABLE "Customers" (
		"customer_id" TEXT,
		"customer_postal_code" INTEGER,
		PRIMARY KEY("customer_id")
	);

	--olist_sellers_dataset.csv
	CREATE TABLE "Sellers" (
		"seller_id" TEXT,
		"seller_postal_code" INTEGER,
		PRIMARY KEY("seller_id")
	);

	--olist_orders_dataset.csv
	CREATE TABLE "Orders" (
		"order_id" TEXT,
		"customer_id" TEXT,
		PRIMARY KEY("order_id"),
		FOREIGN KEY("customer_id") REFERENCES "Customers"("customer_id")
	);

	--olist_order_items_dataset.csv
	CREATE TABLE "Order_items" (
		"order_id" TEXT,
		"order_item_id" INTEGER,
		"product_id" TEXT,
		"seller_id" TEXT,
		PRIMARY KEY("order_id", "order_item_id", "product_id", "seller_id"),
		FOREIGN KEY("seller_id") REFERENCES "Sellers"("seller_id")
		FOREIGN KEY("order_id") REFERENCES "Orders"("order_id")
	);
	"""
    )

    shutil.copyfile(src="A3Small.db", dst="A3Medium.db")
    shutil.copyfile(src="A3Small.db", dst="A3Large.db")

    medium = sqlite3.connect("A3Medium.db")
    large = sqlite3.connect("A3Large.db")

    add_data(small, "Small")
    add_data(medium, "Medium")
    add_data(large, "Large")

    small.close()
    medium.close()
    large.close()


if __name__ == "__main__":
    main()
