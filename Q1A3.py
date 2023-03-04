import scenario

import sqlite3


def main():
    with scenario.set_scenario("Uninformed") as (small, medium, large):
        small.execute(
            """
            SELECT customer_postal_code
                FROM Customers
                ORDER BY RANDOM()
                LIMIT 1;
            """
        )

        random_postal_code = small.fetchone()[0]

        print(question1(small, random_postal_code))
        print(question1(medium, random_postal_code))
        print(question1(large, random_postal_code))

    with scenario.set_scenario("Self-Optimized") as (small, medium, large):
        print(question1(small, random_postal_code))
        print(question1(medium, random_postal_code))
        print(question1(large, random_postal_code))

    with scenario.set_scenario("User-Optimized") as (small, medium, large):
        print(question1(small, random_postal_code))
        print(question1(medium, random_postal_code))
        print(question1(large, random_postal_code))


def question1(cursor: sqlite3.Cursor, postal_code: str) -> int:
    """
    Given a random customer_postal_code from Customers, find how many orders
    containing more than 1 item have been placed by customers who have that
    customer_postal_code.
    """

    # TODO: this is always returning 0.

    cursor.execute(
        f"""
        SELECT COUNT(*)
            FROM Orders o, Customers c
            WHERE
                o.customer_id = c.customer_id AND
                c.customer_postal_code = {postal_code} AND
                (
                    SELECT COUNT(order_item_id)
                        FROM Order_items oi
                        WHERE
                            o.order_id = oi.order_id
                ) > 1;
        """
    )

    """
    SELECT oi.order_id
        FROM Order_items oi, Orders o
        WHERE
        	oi.order_id = o.order_id
        GROUP BY oi.order_id
        HAVING COUNT(*) > 1;
    """

    return cursor.fetchone()[0]


if __name__ == "__main__":
    main()
