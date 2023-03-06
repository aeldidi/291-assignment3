import scenario

import time
import sqlite3


def main():
    with scenario.set_scenario("Q2", "Uninformed") as (small, medium, large):
        create_view(small.cursor(), small)
        create_view(medium.cursor(), medium)
        create_view(large.cursor(), large)

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

            small_res = question2(small.cursor(), small, random_postal_code)
            medium_res = question2(medium.cursor(), medium, random_postal_code)
            large_res = question2(large.cursor(), large, random_postal_code)
            print(f"{small_res}, {medium_res}, {large_res}")

    with scenario.set_scenario("Q2", "Self-Optimized") as (small, medium, large):
        create_view(small.cursor(), small)
        create_view(medium.cursor(), medium)
        create_view(large.cursor(), large)

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

            small_res = question2(small.cursor(), small, random_postal_code)
            medium_res = question2(medium.cursor(), medium, random_postal_code)
            large_res = question2(large.cursor(), large, random_postal_code)
            print(f"{small_res}, {medium_res}, {large_res}")

    with scenario.set_scenario("Q2", "User-Optimized") as (small, medium, large):
        create_view(small.cursor(), small)
        create_view(medium.cursor(), medium)
        create_view(large.cursor(), large)

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

            small_res = question2(small.cursor(), small, random_postal_code)
            medium_res = question2(medium.cursor(), medium, random_postal_code)
            large_res = question2(large.cursor(), large, random_postal_code)
            print(f"{small_res}, {medium_res}, {large_res}")
    scenario.generate_plot("Q2")


def create_view(cursor: sqlite3.Cursor, conn: sqlite3.Connection):
    cursor.execute(
        f"""
        CREATE VIEW OrderSize AS
        SELECT o.order_id, COUNT(DISTINCT oi.order_item_id) AS num_items
        FROM Orders o, Customers c, Order_items oi
        WHERE
        	o.customer_id = c.customer_id AND
        	oi.order_id = o.order_id
        GROUP BY o.order_id;
        """
    )
    conn.commit()


def question2(
    cursor: sqlite3.Cursor, conn: sqlite3.Connection, postal_code: str
) -> int:
    """
    Create a VIEW called OrderSize which has two columns, oid and size, where
    oid is an order_id and size is the total number of items in that order.
    Using the view OrderSize, extend Q1 with the orders that have items more
    than the average number of items in the orders.
    """
    start_time = time.perf_counter_ns()

    cursor.execute(
        f"""
        SELECT COUNT(DISTINCT oi.order_id)
        FROM Orders AS o
        JOIN Order_items AS oi ON oi.order_id = o.order_id
        WHERE o.customer_id IN (
            SELECT c.customer_id 
            FROM Customers AS c 
            WHERE c.customer_postal_code = 30494
        )
        AND oi.order_id IN (
            SELECT oi2.order_id
            FROM Order_items AS oi2
            GROUP BY oi2.order_id
            HAVING COUNT(DISTINCT oi2.order_item_id) > (
        		SELECT AVG(num_items)
        			FROM OrderSize
        		)
        )
        """
    )
    conn.commit()

    elapsed = time.perf_counter_ns() - start_time
    return elapsed / 1e6


if __name__ == "__main__":
    main()
