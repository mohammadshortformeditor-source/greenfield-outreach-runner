from __future__ import annotations

import os
from pathlib import Path

import runner_once as base


_original_load_transport = base._load_transport


def _load_canary_transport():
    transport = _original_load_transport()
    engine_root = Path(os.environ.get("GFO_ENGINE_ROOT", "engine")).resolve()
    launcher = engine_root / "tools" / "operator_runtime_r0007_target_routecanary.py"
    if not launcher.exists():
        raise RuntimeError(f"route canary launcher not found: {launcher}")
    transport.LAUNCHER = launcher
    return transport


def main() -> int:
    title = os.environ.get("GFO_PUBLIC_TRIGGER_TITLE", "")
    if title.startswith("[GFO-CANARY]"):
        os.environ["GFO_PUBLIC_TRIGGER_TITLE"] = title.replace(
            "[GFO-CANARY]", "[GFO-RUN]", 1
        )
    base._load_transport = _load_canary_transport
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
