#!/usr/bin/env python3
"""Vacuum: reclaim disk space from deleted or overwritten entries."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Use a small file-rotation threshold so vacuum has something to do.
        db = bc.DB.open(tmpdir, max_file_bytes=256)

        # Write data, then overwrite / delete to create dead bytes.
        for i in range(50):
            db.put(f"key:{i:04d}".encode(), b"x" * 64, sync=False)

        # Overwrite half the keys (old values become dead space).
        for i in range(25):
            db.put(f"key:{i:04d}".encode(), b"y" * 64, sync=False)

        # Delete some more.
        for i in range(25, 35):
            db.delete(f"key:{i:04d}".encode(), sync=False)

        # --- Vacuum with default options ---
        print("=== Vacuum (defaults) ===")
        vacuumed = db.vacuum()
        print(f"  vacuum() returned {vacuumed}")

        # Run vacuum in a loop until no more files need compaction.
        passes = 0
        while db.vacuum():
            passes += 1
        print(f"  Additional passes: {passes}")

        # --- Vacuum with custom options ---
        print("\n=== Vacuum (custom options) ===")
        passes = 0
        while db.vacuum(fragmentation_threshold=0.1, absorb_threshold=1024 * 1024):
            passes += 1
        print(f"  Passes with low threshold: {passes}")

        # Verify data is still intact.
        print("\n=== Verify data after vacuum ===")
        alive = sum(1 for _ in db.keys())
        print(f"  Live keys remaining: {alive}")
        print(f"  key:0000 = {db.get(b'key:0000')}")  # b'y'*64
        print(f"  key:0030 = {db.get(b'key:0030')}")  # None (deleted)
        print(f"  key:0040 = {db.get(b'key:0040')}")  # b'x'*64

        print("\nDone.")


if __name__ == "__main__":
    main()
