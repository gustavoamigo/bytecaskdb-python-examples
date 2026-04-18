#!/usr/bin/env python3
"""Basic ByteCaskDB usage: open, CRUD, batch, close."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db = bc.DB.open(tmpdir)

        # --- Single-key operations ---
        db[b"user:1"] = b"alice"
        db[b"user:2"] = b"bob"
        db[b"user:3"] = b"carol"

        val = db.get(b"user:1")
        print(f"get(user:1) = {val}")  # b'alice'

        exists = b"user:2" in db
        print(f"'user:2' in db = {exists}")  # True

        del db[b"user:3"]
        print(f"del db[b'user:3']")

        gone = db.get(b"user:3")
        print(f"get(user:3) after delete = {gone}")  # None

        # --- Batch operations ---
        with db.batch() as b:
            b[b"user:10"] = b"dave"
            b[b"user:11"] = b"eve"
            del b[b"user:1"]

        print(f"\nAfter batch:")
        print(f"  get(user:1) = {db.get(b'user:1')}")  # None (deleted)
        print(f"  get(user:10) = {db.get(b'user:10')}")  # b'dave'
        print(f"  get(user:11) = {db.get(b'user:11')}")  # b'eve'

        # --- Write options via keyword arguments ---
        db.put(b"fast_key", b"fast_value", sync=False)
        print(f"\nNoSync write: get(fast_key) = {db.get(b'fast_key')}")

        # --- Degraded state check ---
        print(f"\nis_degraded = {db.is_degraded}")  # False
        print(f"degraded_reason = '{db.degraded_reason}'")  # ''

        print("\nDone.")


if __name__ == "__main__":
    main()
