#!/usr/bin/env python3
"""Transaction guards: ensure_absent, ensure_present, ensure_range_unchanged."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db = bc.DB.open(tmpdir)
        db[b"user:alice"] = b"Alice"
        db[b"user:bob"] = b"Bob"

        # --- ensure_absent: key must NOT exist at commit time ---
        print("=== ensure_absent ===")
        with db.transaction() as txn:
            txn.ensure_absent(b"user:carol")   # guard: carol must not exist
            txn[b"user:carol"] = b"Carol"      # then create her
        print(f"  Create carol (absent guard): committed")

        # Now try again — carol already exists, so the guard fails.
        try:
            with db.transaction() as txn:
                txn.ensure_absent(b"user:carol")
                txn[b"user:carol"] = b"Carol v2"
        except bc.ConflictError:
            print(f"  Create carol again:          ConflictError (already exists)")

        # --- ensure_present: key MUST exist at commit time ---
        print("\n=== ensure_present ===")
        with db.transaction() as txn:
            txn.ensure_present(b"user:alice")  # guard: alice must exist
            txn[b"user:alice"] = b"Alice Updated"
        print(f"  Update alice (present guard): committed")

        # Delete alice, then try ensure_present — should conflict.
        del db[b"user:alice"]
        try:
            with db.transaction() as txn:
                txn.ensure_present(b"user:alice")
                txn[b"user:alice"] = b"Alice Revived"
        except bc.ConflictError:
            print(f"  Update deleted alice:         ConflictError")

        # --- ensure_range_unchanged: no key in range changed since snap ---
        print("\n=== ensure_range_unchanged ===")
        db[b"user:alice"] = b"Alice"  # restore
        with db.transaction() as txn:
            # Guard: the entire user:a..user:c range is unchanged since snapshot.
            txn.ensure_range_unchanged(b"user:a", b"user:c")
            txn[b"user:audit"] = b"checked a-c range"
        print(f"  Range unchanged (no edits):   committed")

        # Now mutate within the range, then try the same guard.
        try:
            with db.transaction() as txn:
                # Concurrent write within the guarded range
                db[b"user:bob"] = b"Bob Modified"
                txn.ensure_range_unchanged(b"user:a", b"user:c")
                txn[b"user:audit"] = b"should fail"
        except bc.ConflictError:
            print(f"  Range changed (bob edited):   ConflictError")

        print("\nDone.")


if __name__ == "__main__":
    main()
