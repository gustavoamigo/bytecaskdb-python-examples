#!/usr/bin/env python3
"""Range deletions: delete_range on DB and in a batch."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db = bc.DB.open(tmpdir)

        # Seed data
        for i in range(1, 11):
            db.put(f"log:{i:03d}".encode(), f"event_{i}".encode(), sync=False)

        print("=== Before delete_range ===")
        for key, value in db.items():
            print(f"  {key} -> {value}")

        # --- DB.delete_range: delete keys in [from_key, to_key) ---
        # This deletes log:004 through log:006 (half-open range).
        print("\n=== delete_range(b'log:004', b'log:007') ===")
        db.delete_range(b"log:004", b"log:007")

        for key, value in db.items():
            print(f"  {key} -> {value}")

        # --- Batch: atomic range deletion + put ---
        print("\n=== Batch: delete_range + put ===")
        with db.batch() as b:
            b.delete_range(b"log:008", b"log:011")  # remove log:008..010
            b[b"log:summary"] = b"kept 1-3, 7"

        for key, value in db.items():
            print(f"  {key} -> {value}")

        print("\nDone.")


if __name__ == "__main__":
    main()
