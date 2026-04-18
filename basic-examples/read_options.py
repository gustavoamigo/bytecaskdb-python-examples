#!/usr/bin/env python3
"""ReadOptions: CRC verification on reads."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db = bc.DB.open(tmpdir)
        db.put(b"hello", b"world")

        # --- Default: no checksum verification (fast) ---
        val = db.get(b"hello")
        print(f"Default read:   get(hello) = {val}")

        # --- ReadOptions with CRC verification enabled ---
        ropts = bc.ReadOptions()
        ropts.verify_checksums = True

        val = db.get(b"hello", ropts)
        print(f"Verified read:  get(hello) = {val}")

        # ReadOptions also applies to iterators.
        print("\n=== Verified iteration ===")
        db.put(b"a", b"1")
        db.put(b"b", b"2")
        db.put(b"c", b"3")

        for key, value in db.iter_from(b"", ropts):
            print(f"  {key} -> {value}  (CRC checked)")

        print("\nDone.")


if __name__ == "__main__":
    main()
