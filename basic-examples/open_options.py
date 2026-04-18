#!/usr/bin/env python3
"""Open options: file rotation, recovery threads, CRC policy."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        # --- Options: configure how the database opens ---
        opts = bc.Options()

        # Active-file rotation threshold.
        # When the active data file exceeds this size, a new file is started.
        # Default is 64 MiB. A smaller value creates more files (useful for
        # faster vacuum cycles at the cost of more file handles).
        opts.max_file_bytes = 4 * 1024 * 1024  # 4 MiB

        # Number of threads used during recovery (hint-file replay at open).
        # Default is 4.
        opts.recovery_threads = 2

        # If True (default), CRC errors found during recovery cause an
        # exception.  Set to False to silently skip corrupted entries.
        opts.fail_recovery_on_crc_errors = True

        db = bc.DB.open(tmpdir, opts)

        wopts = bc.WriteOptions()
        wopts.sync = False
        for i in range(100):
            db.put(f"k:{i}".encode(), b"v" * 512, wopts)

        print(f"Wrote 100 keys with max_file_bytes={opts.max_file_bytes}")

        # Re-open the database to exercise recovery.
        del db
        db = bc.DB.open(tmpdir, opts)
        print(f"Re-opened — recovery_threads={opts.recovery_threads}")
        print(f"  get(k:0) = {db.get(b'k:0')}")
        print(f"  get(k:99) = {db.get(b'k:99')}")

        print("\nDone.")


if __name__ == "__main__":
    main()
