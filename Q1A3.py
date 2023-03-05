import scenario

import timeit
import sqlite3


def main():
    with scenario.set_scenario("Q1", "Uninformed") as (small, medium, large):
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

    with scenario.set_scenario("Q1", "Self-Optimized") as (small, medium, large):
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

    with scenario.set_scenario("Q1", "User-Optimized") as (small, medium, large):
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
    Given a random customer_postal_code from Customers, find how many orders
    containing more than 1 item have been placed by customers who have that
    customer_postal_code.
    """
    start_time = timeit.default_timer()

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

    elapsed = timeit.default_timer() - start_time
    return elapsed


if __name__ == "__main__":
    main()
