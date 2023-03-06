import sqlite3
import random
import shutil
from typing import Literal
from pathlib import Path
import csv
import os
import contextlib
import sys

"""
Creates the three files A3Small.db, A3Medium.db, and A3Large.db by randomly
populating them with data as described in assignment 3 given the scenario
specified.

import this from your files and run scenario.set_scenario, giving it one of the
following:

- 'Uninformed'
- 'Self-Optimized'
- 'User-Optimized'
"""

Scenario = Literal["Uninformed", "Self-Optimized", "User-Optimized"]

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

import matplotlib.pyplot as plt
import numpy as np
import os.path
from os import path
import csv


def generate_plot(question):
    parser(question)
    plot(question)


def parser(question_num):
    # {DB:[[User-Optimized], [Self-Optimized], [Uninformed]]}
    data = {"SmallDB": [[], [], []], "MediumDB": [[], [], []], "LargeDB": [[], [], []]}
    # Parses output data into data dictionary
    files = ["User-Optimized.csv", "Self-Optimized.csv", "Uninformed.csv"]
    try:
        for index, element in enumerate(files):
            with open(f"{question_num}-{element}") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=",")
                line_count = 0
                for row in csv_reader:
                    if line_count == 0:
                        line_count = 1
                    else:
                        data["SmallDB"][index].append(float(row[0]))
                        data["MediumDB"][index].append(float(row[1]))
                        data["LargeDB"][index].append(float(row[2]))
                # Takes average of all test runs for each scenario
                data["SmallDB"][index] = sum(data["SmallDB"][index]) / len(
                    data["SmallDB"][index]
                )
                data["MediumDB"][index] = sum(data["MediumDB"][index]) / len(
                    data["MediumDB"][index]
                )
                data["LargeDB"][index] = sum(data["LargeDB"][index]) / len(
                    data["LargeDB"][index]
                )
    except:
        print("File name not found or incorrect format.")
        exit()
    return data


def plot(question_num):
    dataBases = (
        "SmallDB",
        "MediumDB",
        "LargeDB",
    )
    data = parser(question_num)
    weight_counts = {
        "Uninformed": np.array(
            [data["SmallDB"][2], data["MediumDB"][2], data["LargeDB"][2]]
        ),
        "Self-Optimized": np.array(
            [data["SmallDB"][1], data["MediumDB"][1], data["LargeDB"][1]]
        ),
        "User-Optimized": np.array(
            [data["SmallDB"][0], data["MediumDB"][0], data["LargeDB"][0]]
        ),
    }
    # Width of each bar
    width = 0.5
    # Define custom colors for each label in the legend
    colors = {
        "User-Optimized": "#FFC000",
        "Self-Optimized": "#FF0000",
        "Uninformed": "#0070C0",
    }
    # Create figure and axis objects
    fig, ax = plt.subplots()
    # Initialize the bottom of the stacked bar to zero
    bottom = np.zeros(3)
    # Loop through each label in the legend
    for label, weight_count in weight_counts.items():
        # Set the color for this label based on the custom color dictionary
        color = colors.get(label, "gray")

        # Plot the bar with the custom color
        ax.bar(dataBases, weight_count, width, label=label, bottom=bottom, color=color)

        # Update the bottom of the stacked bar for the next iteration
        bottom += weight_count
    # Set title and legend
    number = question_num.replace("Q", "", 1)
    ax.set_title(f"Query {number} (runtime in ms)")
    ax.legend(loc="upper left")
    # Display the graph
    image_file = f"{question_num}A3chart.png"
    if path.exists(image_file):
        os.remove(image_file)
    plt.savefig(f"{question_num}A3chart.png")


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


@contextlib.contextmanager
def set_scenario(
    question: str,
    scenario: Scenario,
) -> tuple[sqlite3.Connection, sqlite3.Connection, sqlite3.Connection]:
    if Path("A3Small.db").exists():
        os.remove("A3Small.db")
    if Path("A3Medium.db").exists():
        os.remove("A3Medium.db")
    if Path("A3Large.db").exists():
        os.remove("A3Large.db")

    small = sqlite3.connect("A3Small.db")
    create_tables(small, scenario)

    shutil.copyfile(src="A3Small.db", dst="A3Medium.db")
    shutil.copyfile(src="A3Small.db", dst="A3Large.db")

    medium = sqlite3.connect("A3Medium.db")
    large = sqlite3.connect("A3Large.db")

    add_data(small, "Small")
    add_data(medium, "Medium")
    add_data(large, "Large")

    cursors = small, medium, large

    old_stdout = sys.stdout
    sys.stdout = open(question + "-" + scenario + ".csv", "w")
    yield cursors
    sys.stdout.close()
    sys.stdout = old_stdout

    small.commit()
    medium.commit()
    large.commit()

    small.close()
    medium.close()
    large.close()


def create_indicies(conn, scenario: Scenario):
    if scenario == "Uninformed":
        return
    elif scenario == "Self-Optimized":
        return

    conn.executescript(
        """
        CREATE UNIQUE INDEX customer_index
            ON Customers(customer_id);

        CREATE UNIQUE INDEX sellers_index
            ON Sellers(seller_id);

        CREATE UNIQUE INDEX orders_index
            ON Orders(order_id);

        CREATE UNIQUE INDEX order_items_index
            ON Order_items(order_id, order_item_id, product_id, seller_id);
        """
    )


def create_tables(conn, scenario: Scenario):
    c = conn.cursor()
    if scenario == "Uninformed":
        c.executescript(
            """
	        -- olist_customers_dataset.csv
	        CREATE TABLE "Customers" (
	        	"customer_id" TEXT,
	        	"customer_postal_code" INTEGER
	        );

	        --olist_sellers_dataset.csv
	        CREATE TABLE "Sellers" (
	        	"seller_id" TEXT,
	        	"seller_postal_code" INTEGER
	        );

	        --olist_orders_dataset.csv
	        CREATE TABLE "Orders" (
	        	"order_id" TEXT,
	        	"customer_id" TEXT
	        );

	        --olist_order_items_dataset.csv
	        CREATE TABLE "Order_items" (
	        	"order_id" TEXT,
	        	"order_item_id" INTEGER,
	        	"product_id" TEXT,
	        	"seller_id" TEXT
	        );

            PRAGMA automatic_index = false;
	        """
        )
    elif scenario == "Self-Optimized":
        c.executescript(
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

            PRAGMA automatic_index = true;
	        """
        )
    else:
        c.executescript(
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

            PRAGMA automatic_index = true;
	        """
        )
    conn.commit()
