#!/usr/bin/env python3
"""ReadOptions: CRC verification on reads via verify_checksums keyword."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db = bc.DB.open(tmpdir)
        db[b"hello"] = b"world"

        # --- Default: no checksum verification (fast) ---
        val = db.get(b"hello")
        print(f"Default read:   get(hello) = {val}")

        # --- CRC verification enabled via keyword argument ---
        val = db.get(b"hello", verify_checksums=True)
        print(f"Verified read:  get(hello) = {val}")

        # verify_checksums also applies to iterators.
        print("\n=== Verified iteration ===")
        db[b"a"] = b"1"
        db[b"b"] = b"2"
        db[b"c"] = b"3"

        for key, value in db.items(b"", verify_checksums=True):
            print(f"  {key} -> {value}  (CRC checked)")

        print("\nDone.")


if __name__ == "__main__":
    main()
