#!/usr/bin/env python3
"""Open options: file rotation, recovery threads, CRC policy."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        # --- Open with keyword options ---
        db = bc.DB.open(
            tmpdir,
            max_file_bytes=4 * 1024 * 1024,  # 4 MiB (default 64 MiB)
            recovery_threads=2,                # default 4
            fail_recovery_on_crc_errors=True,  # default True
        )

        for i in range(100):
            db.put(f"k:{i}".encode(), b"v" * 512, sync=False)

        print(f"Wrote 100 keys with max_file_bytes=4 MiB")

        # Re-open the database to exercise recovery.
        del db
        db = bc.DB.open(
            tmpdir,
            max_file_bytes=4 * 1024 * 1024,
            recovery_threads=2,
        )
        print(f"Re-opened — recovery_threads=2")
        print(f"  get(k:0) = {db.get(b'k:0')}")
        print(f"  get(k:99) = {db.get(b'k:99')}")

        print("\nDone.")


if __name__ == "__main__":
    main()
