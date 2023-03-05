import scenario

import time
import sqlite3


def main():
    with scenario.set_scenario("Q4", "Uninformed") as (small, medium, large):
        print("SmallDB, MediumDB, LargeDB")
        for i in range(50):
            c = small.cursor()
            c.execute(
                """
                SELECT customer_id
                    FROM OrderSize
                    WHERE num_items > 1
                    ORDER BY RANDOM()
                    LIMIT 1;
                """
            )
            small.commit()
            random_customer = c.fetchone()[0]

            small_res = question4(small.cursor(), small, random_customer)
            medium_res = question4(medium.cursor(), medium, random_customer)
            large_res = question4(large.cursor(), large, random_customer)
            print(f"{small_res}, {medium_res}, {large_res}")

    with scenario.set_scenario("Q4", "Self-Optimized") as (small, medium, large):
        print("SmallDB, MediumDB, LargeDB")
        for i in range(50):
            c = small.cursor()
            c.execute(
                """
                SELECT customer_id
                    FROM OrderSize
                    WHERE num_items > 1
                    ORDER BY RANDOM()
                    LIMIT 1;
                """
            )
            small.commit()
            random_customer = c.fetchone()[0]

            small_res = question4(small.cursor(), small, random_customer)
            medium_res = question4(medium.cursor(), medium, random_customer)
            large_res = question4(large.cursor(), large, random_customer)
            print(f"{small_res}, {medium_res}, {large_res}")

    with scenario.set_scenario("Q4", "User-Optimized") as (small, medium, large):
        print("SmallDB, MediumDB, LargeDB")
        for i in range(50):
            c = small.cursor()
            c.execute(
                """
                SELECT customer_id
                    FROM OrderSize
                    WHERE num_items > 1
                    ORDER BY RANDOM()
                    LIMIT 1;
                """
            )
            small.commit()
            random_customer = c.fetchone()[0]

            small_res = question4(small.cursor(), small, random_customer)
            medium_res = question4(medium.cursor(), medium, random_customer)
            large_res = question4(large.cursor(), large, random_customer)
            print(f"{small_res}, {medium_res}, {large_res}")


def create_view(cursor: sqlite3.Cursor, conn: sqlite3.Connection):
    cursor.execute(
        f"""
        CREATE VIEW OrderSize AS
        SELECT o.order_id, o.customer_id, COUNT(DISTINCT oi.order_item_id) AS num_items
        FROM Orders o, Customers c, Order_items oi
        WHERE
        	o.customer_id = c.customer_id AND
        	oi.order_id = o.order_id
        GROUP BY o.order_id;
        """
    )
    conn.commit()


def question4(cursor: sqlite3.Cursor, conn: sqlite3.Connection, customer: str) -> int:
    """
    Choose a random customer with more than one order and for that customer's
    orders, find in how many (unique) postal codes the sellers provided those
    orders.
    """
    start_time = time.perf_counter_ns()

    cursor.execute(
        f"""
        """
    )
    conn.commit()

    elapsed = time.perf_counter_ns() - start_time
    return elapsed / 1e6


if __name__ == "__main__":
    main()
