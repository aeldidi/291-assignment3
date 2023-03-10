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
    scenario.generate_plot("Q4")


def question4(
    cursor: sqlite3.Cursor, conn: sqlite3.Connection, postal_code: str
) -> int:
    """
    Choose a random customer with more than one order and for that customer's
    orders, find in how many (unique) postal codes the sellers provided those
    orders.
    """
    start_time = time.perf_counter_ns()

    # NOTE: Each customer_id is only associated with one order. Since the
    #       question asks about customers with more than one order, we assume
    #       that a postal code with more than one order is a customer with more
    #       than one order.
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
