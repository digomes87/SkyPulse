import io
import subprocess
import sys
import zipfile
from pathlib import Path

from skypulse.deployment.lambdas import FUNCTIONS, build_zip


def test_zip_holds_only_the_rules_and_the_handlers() -> None:
    names = zipfile.ZipFile(io.BytesIO(build_zip())).namelist()

    assert "skypulse/__init__.py" in names
    assert "skypulse/eu261/rules.py" in names
    assert "skypulse/lambdas/eu261_calculator.py" in names
    assert {name.split("/")[1] for name in names} == {"__init__.py", "eu261", "lambdas"}


def test_handlers_import_with_the_standard_library_only(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle.zip"
    bundle.write_bytes(build_zip())
    # -I -S: no user site, no site-packages. boto3 is stubbed like the runtime would provide it.
    stub = tmp_path / "stubs"
    stub.mkdir()
    (stub / "boto3.py").write_text("def resource(*args, **kwargs): ...\n")
    modules = ", ".join(spec.handler.rsplit(".", 1)[0] for spec in FUNCTIONS)
    code = f"import sys; sys.path[:0] = [{str(bundle)!r}, {str(stub)!r}]; import {modules}"

    result = subprocess.run(
        [sys.executable, "-I", "-S", "-c", code], check=False, capture_output=True, text=True
    )

    assert result.returncode == 0, result.stderr
