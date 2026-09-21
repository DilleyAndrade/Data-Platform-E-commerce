import sys
import tempfile
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"

for path in (str(PROJECT_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture
def tmp_path():
    """Temporary directory inside the writable workspace on restricted Windows hosts."""
    base = PROJECT_ROOT / "tests" / ".tmp"
    base.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=base) as directory:
        yield Path(directory)
