#!/usr/bin/env python3
"""Degraded state: checking is_degraded, degraded_reason, and resume()."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db = bc.DB.open(tmpdir)
        db.put(b"key", b"value")

        # --- Check degraded status ---
        print("=== Degraded state check ===")
        print(f"  is_degraded    = {db.is_degraded}")
        print(f"  degraded_reason = '{db.degraded_reason}'")

        # --- Using DbDegraded exception ---
        # DbDegraded is a RuntimeError subclass raised when the engine
        # enters a degraded state (e.g. disk full, I/O errors).
        # In normal operation this won't trigger, but here's how to handle it:
        print("\n=== Handling DbDegraded ===")
        try:
            # Normal operation — this won't raise.
            db.put(b"another", b"value")
            print("  Write succeeded (engine healthy)")
        except bc.DbDegraded as e:
            print(f"  Engine degraded: {e}")
            print(f"  Reason: {db.degraded_reason}")
            # Attempt recovery
            try:
                db.resume()
                print("  resume() succeeded — engine recovered")
            except Exception as e2:
                print(f"  resume() failed: {e2}")

        # --- IoError handling ---
        # IoError is an alias for Python's built-in OSError.
        # Raised on I/O failures (e.g. permission denied, disk errors).
        print("\n=== IoError (OSError) ===")
        print(f"  bc.IoError is OSError: {bc.IoError is OSError}")
        try:
            # Trying to open a non-existent nested path will raise.
            bc.DB.open("/nonexistent/path/db")
        except bc.IoError as e:
            print(f"  Caught IoError: {e}")

        print("\nDone.")


if __name__ == "__main__":
    main()
