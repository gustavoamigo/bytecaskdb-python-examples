#!/usr/bin/env python3
"""Range deletions: del_range on DB and WritePlan.del_range in a batch."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db = bc.DB.open(tmpdir)

        # Seed data
        opts = bc.WriteOptions()
        opts.sync = False
        for i in range(1, 11):
            db.put(f"log:{i:03d}".encode(), f"event_{i}".encode(), opts)

        print("=== Before del_range ===")
        for key, value in db.iter_from():
            print(f"  {key} -> {value}")

        # --- DB.del_range: delete keys in [from_key, to_key) ---
        # This deletes log:004 through log:006 (half-open range).
        print("\n=== del_range(b'log:004', b'log:007') ===")
        db.del_range(b"log:004", b"log:007")

        for key, value in db.iter_from():
            print(f"  {key} -> {value}")

        # --- WritePlan.del_range: atomic range deletion in a batch ---
        print("\n=== Batch: del_range + put ===")
        plan = bc.WritePlan()
        plan.del_range(b"log:008", b"log:011")  # remove log:008..010
        plan.put(b"log:summary", b"kept 1-3, 7")
        db.apply_batch(plan)

        for key, value in db.iter_from():
            print(f"  {key} -> {value}")

        print("\nDone.")


if __name__ == "__main__":
    main()
