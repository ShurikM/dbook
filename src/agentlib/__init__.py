"""Compatibility shim: re-export the ``lib`` package as ``agentlib``.

The pip package *agentlib* (https://github.com/barkain/agentlib) installs its
Python module under the legacy name ``lib``.  This shim lets dbook import it
as ``agentlib`` so the import path matches the package name.

Once upstream ships a proper ``agentlib`` top-level module, this shim can be
removed.
"""

from __future__ import annotations

import importlib as _importlib
import sys as _sys

# Import the real ``lib`` package.
_lib = _importlib.import_module("lib")  # type: ignore[import-untyped]

# Re-export every public name.
for _name in dir(_lib):
    if not _name.startswith("_"):
        globals()[_name] = getattr(_lib, _name)

# Eagerly import all lib sub-modules and register agentlib.* aliases so that
# ``from agentlib.llm import X`` works without individual shim files.
_SUBMODULES = [
    "chunker",
    "llm",
    "metadata",
    "models",
    "parser",
    "storage",
    "summariser",
]
for _sub in _SUBMODULES:
    try:
        _mod = _importlib.import_module(f"lib.{_sub}")
        _sys.modules[f"agentlib.{_sub}"] = _mod
    except ImportError:
        pass
