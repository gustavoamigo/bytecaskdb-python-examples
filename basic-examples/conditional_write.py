#!/usr/bin/env python3
"""Transactions with conflict detection: snapshot isolation, retry loops."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db = bc.DB.open(tmpdir)

        # Seed some data
        db[b"stock:widget"] = b"100"
        db[b"price:widget"] = b"25"

        # --- Example 1: Decrement stock in a transaction ---
        print("=== Decrement stock ===")
        with db.transaction() as txn:
            stock = int(txn[b"stock:widget"])
            txn[b"stock:widget"] = str(stock - 1).encode()
        print(f"  Stock decremented: {stock} -> {stock - 1}")
        print(f"  Current stock: {db.get(b'stock:widget')}")

        # --- Example 2: Conflict detection ---
        print("\n=== Conflict detection ===")
        try:
            with db.transaction() as txn:
                stock = int(txn[b"stock:widget"])

                # Simulate a concurrent writer
                db[b"stock:widget"] = b"50"
                print(f"  Concurrent writer set stock to 50")

                txn[b"stock:widget"] = str(stock - 1).encode()
            # __exit__ calls commit — conflict detected, raises ConflictError
        except bc.ConflictError:
            print(f"  ConflictError raised — transaction aborted")
        print(f"  Stock unchanged at: {db.get(b'stock:widget')}")  # b'50'

        # --- Example 3: ensure_unchanged for read dependencies ---
        print("\n=== Order at current price ===")
        with db.transaction() as txn:
            price = int(txn[b"price:widget"])
            order_total = str(price * 3).encode()  # buy 3 widgets
            txn.ensure_unchanged(b"price:widget")  # guard: reject if price changed
            txn[b"order:001"] = order_total
        print(f"  Order placed: 3 x ${price} = ${price * 3}")

        # --- Example 4: Retry loop ---
        print("\n=== Retry loop ===")
        for attempt in range(3):
            try:
                with db.transaction() as txn:
                    current = int(txn[b"stock:widget"])
                    if current <= 0:
                        print("  Out of stock!")
                        break
                    txn[b"stock:widget"] = str(current - 1).encode()
                print(f"  Attempt {attempt + 1}: success, stock {current} -> {current - 1}")
                break
            except bc.ConflictError:
                print(f"  Attempt {attempt + 1}: conflict, retrying...")

        print("\nDone.")


if __name__ == "__main__":
    main()
