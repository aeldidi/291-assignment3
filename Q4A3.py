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

            small_res = question1(small.cursor(), small, random_postal_code)
            medium_res = question1(medium.cursor(), medium, random_postal_code)
            large_res = question1(large.cursor(), large, random_postal_code)
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

            small_res = question1(small.cursor(), small, random_postal_code)
            medium_res = question1(medium.cursor(), medium, random_postal_code)
            large_res = question1(large.cursor(), large, random_postal_code)
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

            small_res = question1(small.cursor(), small, random_postal_code)
            medium_res = question1(medium.cursor(), medium, random_postal_code)
            large_res = question1(large.cursor(), large, random_postal_code)
            print(f"{small_res}, {medium_res}, {large_res}")


def question1(
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
        SELECT COUNT(DISTINCT oi.order_id)
        FROM Orders AS o
        JOIN Order_items AS oi ON oi.order_id = o.order_id
        WHERE o.customer_id IN (
            SELECT c.customer_id 
            FROM Customers AS c 
            WHERE c.customer_postal_code = {postal_code}
        )
        AND oi.order_id IN (
            SELECT oi2.order_id
            FROM Order_items AS oi2
            GROUP BY oi2.order_id
            HAVING COUNT(DISTINCT oi2.order_item_id) > 1
        )
        """
    )
    conn.commit()

    elapsed = time.perf_counter_ns() - start_time
    return elapsed / 1e6


if __name__ == "__main__":
    main()
