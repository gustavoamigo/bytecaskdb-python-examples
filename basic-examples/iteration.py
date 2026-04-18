#!/usr/bin/env python3
"""Iteration patterns: forward, reverse, prefix scan, keys-only."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db = bc.DB.open(tmpdir)

        # Seed data
        for i in range(1, 6):
            db.put(f"user:{i}".encode(), f"name_{i}".encode(), sync=False)
        for i in range(1, 4):
            db.put(f"product:{i}".encode(), f"item_{i}".encode(), sync=False)

        # --- Forward scan (all keys) ---
        print("=== All entries (forward) ===")
        for key, value in db.items():
            print(f"  {key} -> {value}")

        # --- Prefix scan ---
        print("\n=== Prefix scan: user:* ===")
        for key, value in db.prefix(b"user:"):
            print(f"  {key} -> {value}")

        # --- Keys-only (no disk I/O) ---
        print("\n=== Keys only ===")
        for key in db.keys():
            print(f"  {key}")

        # --- Reverse scan (all keys) ---
        print("\n=== All entries (reverse) ===")
        for key, value in db.ritems():
            print(f"  {key} -> {value}")

        # --- Reverse keys from a specific point ---
        print("\n=== Reverse keys from user:3 ===")
        for key in db.rkeys(b"user:3"):
            print(f"  {key}")

        # --- Snapshot iteration ---
        print("\n=== Snapshot iteration ===")
        db.put(b"user:6", b"name_6", sync=False)

        with db.snapshot() as snap:
            # snap is taken here, after user:6. Show isolation:
            db.put(b"user:7", b"name_7", sync=False)

            snap_keys = list(snap.keys(b"user:"))
            db_keys = list(db.keys(b"user:"))

            print(f"  Snapshot sees {len(snap_keys)} user keys (no user:7)")
            print(f"  Live DB sees {len(db_keys)} user keys (includes user:7)")

        print("\nDone.")


if __name__ == "__main__":
    main()
