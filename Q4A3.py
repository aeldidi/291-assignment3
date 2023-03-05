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
                SELECT customer_postal_code
                    FROM Customers
                    ORDER BY RANDOM()
                    LIMIT 1;
                """
            )
            small.commit()
            random_postal_code = c.fetchone()[0]

            small_res = question4(small.cursor(), small, random_postal_code)
            medium_res = question4(medium.cursor(), medium, random_postal_code)
            large_res = question4(large.cursor(), large, random_postal_code)
            print(f"{small_res}, {medium_res}, {large_res}")

    with scenario.set_scenario("Q4", "Self-Optimized") as (small, medium, large):
        print("SmallDB, MediumDB, LargeDB")
        for i in range(50):
            c = small.cursor()
            c.execute(
                """
                SELECT customer_postal_code
                    FROM Customers
                    ORDER BY RANDOM()
                    LIMIT 1;
                """
            )
            small.commit()
            random_postal_code = c.fetchone()[0]

            small_res = question4(small.cursor(), small, random_postal_code)
            medium_res = question4(medium.cursor(), medium, random_postal_code)
            large_res = question4(large.cursor(), large, random_postal_code)
            print(f"{small_res}, {medium_res}, {large_res}")

    with scenario.set_scenario("Q4", "User-Optimized") as (small, medium, large):
        print("SmallDB, MediumDB, LargeDB")
        for i in range(50):
            c = small.cursor()
            c.execute(
                """
                SELECT customer_postal_code
                    FROM Customers
                    ORDER BY RANDOM()
                    LIMIT 1;
                """
            )
            small.commit()
            random_postal_code = c.fetchone()[0]

            small_res = question4(small.cursor(), small, random_postal_code)
            medium_res = question4(medium.cursor(), medium, random_postal_code)
            large_res = question4(large.cursor(), large, random_postal_code)
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


def question4(
    cursor: sqlite3.Cursor, conn: sqlite3.Connection, postal_code: str
) -> int:
    """
    Choose a random customer with more than one order and for that customer's
    orders, find in how many (unique) postal codes the sellers provided those
    orders.
    """
    start_time = time.perf_counter_ns()

    cursor.execute(
        f"""
        SELECT COUNT(DISTINCT s.seller_postal_code) AS num_seller_postal_codes
        FROM Orders o
        JOIN Order_items oi ON
            oi.order_id = o.order_id
        JOIN Sellers s ON
            s.seller_id = oi.seller_id
        JOIN Customers c ON
            c.customer_id = o.customer_id
        WHERE
            c.customer_postal_code = {postal_code};
        """
    )
    conn.commit()

    elapsed = time.perf_counter_ns() - start_time
    return elapsed / 1e6


if __name__ == "__main__":
    main()
