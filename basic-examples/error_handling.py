#!/usr/bin/env python3
"""Error handling: degraded state, DegradedError, and IoError."""

import tempfile
import bytecaskdb as bc


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db = bc.DB.open(tmpdir)
        db[b"key"] = b"value"

        # --- Check degraded status ---
        print("=== Degraded state check ===")
        print(f"  is_degraded    = {db.is_degraded}")
        print(f"  degraded_reason = '{db.degraded_reason}'")

        # --- Using DegradedError exception ---
        # DegradedError is raised when the engine enters a degraded state
        # (e.g. disk full, I/O errors).
        # In normal operation this won't trigger, but here's how to handle it:
        print("\n=== Handling DegradedError ===")
        try:
            # Normal operation — this won't raise.
            db[b"another"] = b"value"
            print("  Write succeeded (engine healthy)")
        except bc.DegradedError as e:
            print(f"  Engine degraded: {e}")
            print(f"  Reason: {db.degraded_reason}")
            # Attempt recovery
            try:
                db.resume()
                print("  resume() succeeded — engine recovered")
            except Exception as e2:
                print(f"  resume() failed: {e2}")

        # --- IoError handling ---
        # IoError is Python's built-in OSError.
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
