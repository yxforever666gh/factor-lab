#!/usr/bin/env python3
"""Compatibility shim for the renamed de-HermesNative runtime verifier."""

from __future__ import annotations

from verify_de_hermes_native_runtime import main


if __name__ == "__main__":
    raise SystemExit(main())
