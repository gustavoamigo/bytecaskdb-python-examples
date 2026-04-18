#!/usr/bin/env python3
"""WritePlan guards: ensure_absent, ensure_present, ensure_range_unchanged."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db = bc.DB.open(tmpdir)
        db.put(b"user:alice", b"Alice")
        db.put(b"user:bob", b"Bob")

        # --- ensure_absent: key must NOT exist at commit time ---
        print("=== ensure_absent ===")
        snap = db.snapshot()

        plan = bc.WritePlan(snap)
        plan.ensure_absent(b"user:carol")   # guard: carol must not exist
        plan.put(b"user:carol", b"Carol")   # then create her
        committed = db.apply_batch(plan)
        print(f"  Create carol (absent guard): committed = {committed}")  # True

        # Now try again — carol already exists, so the guard fails.
        snap2 = db.snapshot()
        plan2 = bc.WritePlan(snap2)
        plan2.ensure_absent(b"user:carol")
        plan2.put(b"user:carol", b"Carol v2")
        committed2 = db.apply_batch(plan2)
        print(f"  Create carol again:          committed = {committed2}")  # False

        # --- ensure_present: key MUST exist at commit time ---
        print("\n=== ensure_present ===")
        snap3 = db.snapshot()

        plan3 = bc.WritePlan(snap3)
        plan3.ensure_present(b"user:alice")  # guard: alice must exist
        plan3.put(b"user:alice", b"Alice Updated")
        committed3 = db.apply_batch(plan3)
        print(f"  Update alice (present guard): committed = {committed3}")  # True

        # Delete alice, then try ensure_present — should conflict.
        db.del_(b"user:alice")
        snap4 = db.snapshot()
        plan4 = bc.WritePlan(snap4)
        plan4.ensure_present(b"user:alice")
        plan4.put(b"user:alice", b"Alice Revived")
        committed4 = db.apply_batch(plan4)
        print(f"  Update deleted alice:         committed = {committed4}")  # False

        # --- ensure_range_unchanged: no key in range changed since snap ---
        print("\n=== ensure_range_unchanged ===")
        db.put(b"user:alice", b"Alice")  # restore
        snap5 = db.snapshot()

        plan5 = bc.WritePlan(snap5)
        # Guard: the entire user:a..user:c range is unchanged since snap5.
        plan5.ensure_range_unchanged(b"user:a", b"user:c")
        plan5.put(b"user:audit", b"checked a-c range")
        committed5 = db.apply_batch(plan5)
        print(f"  Range unchanged (no edits):   committed = {committed5}")  # True

        # Now mutate within the range, then try the same guard.
        snap6 = db.snapshot()
        db.put(b"user:bob", b"Bob Modified")  # change within [user:a, user:c)

        plan6 = bc.WritePlan(snap6)
        plan6.ensure_range_unchanged(b"user:a", b"user:c")
        plan6.put(b"user:audit", b"should fail")
        committed6 = db.apply_batch(plan6)
        print(f"  Range changed (bob edited):   committed = {committed6}")  # False

        # --- has_snapshot property ---
        print("\n=== WritePlan.has_snapshot ===")
        simple = bc.WritePlan()
        guarded = bc.WritePlan(db.snapshot())
        print(f"  Simple plan has_snapshot:  {simple.has_snapshot}")   # False
        print(f"  Guarded plan has_snapshot: {guarded.has_snapshot}")  # True

        print("\nDone.")


if __name__ == "__main__":
    main()
