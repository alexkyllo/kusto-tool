import os
import shutil
from pathlib import Path

import nox

os.environ.update({"PDM_IGNORE_SAVED_PYTHON": "1"})


@nox.session(python=["3.7", "3.8", "3.9", "3.10"])
def test(session):
    """Run the tests."""
    session.install("pytest")
    session.run("python", "-m", "pytest", "tests/")


@nox.session
def docs(session):
    """Build the documentation."""
    args = session.posargs or ["-W", "-n", "docs", "docs/_build"]

    if session.interactive and not session.posargs:
        args = ["-a", "--watch=docs/_static", "--open-browser", *args]

    builddir = Path("docs", "_build")
    if builddir.exists():
        shutil.rmtree(builddir)

    session.install("-r", "docs/requirements.txt")

    if session.interactive:
        session.run("sphinx-autobuild", *args)
    else:
        session.run("sphinx-build", *args)
