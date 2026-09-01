"""Regenerate the protobuf and gRPC stubs vendored under `rocia_db_sdk._pb`.

Run after any change to `proto/`. The generated files are committed so that
installing the SDK never requires protoc.

    uv run --python 3.10 python scripts/generate_proto.py
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "proto"
OUT_DIR = ROOT / "src" / "rocia_db_sdk" / "_pb"
PACKAGE = "rocia_db_sdk._pb"

# protoc emits imports rooted at the proto path ("from upstream.v1 import ...").
# The stubs live inside the package, so those have to be rewritten to absolute
# package imports or they only resolve when the proto root is on sys.path.
IMPORT_RE = re.compile(r"^(from|import) (upstream\.v1[\w.]*)", re.MULTILINE)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    proto = PROTO_DIR / "upstream" / "v1" / "upstream.proto"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"--proto_path={PROTO_DIR}",
            f"--python_out={OUT_DIR}",
            f"--pyi_out={OUT_DIR}",
            f"--grpc_python_out={OUT_DIR}",
            str(proto),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        return result.returncode

    for path in sorted(OUT_DIR.rglob("*.py")):
        text = path.read_text()
        rewritten = IMPORT_RE.sub(rf"\1 {PACKAGE}.\2", text)
        if rewritten != text:
            path.write_text(rewritten)

    for directory in [OUT_DIR, *(p for p in OUT_DIR.rglob("*") if p.is_dir())]:
        init = directory / "__init__.py"
        if not init.exists():
            init.write_text("")

    generated = sorted(p.relative_to(ROOT).as_posix() for p in OUT_DIR.rglob("*.py*"))
    print("\n".join(generated))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
