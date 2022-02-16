import os
import shutil
from pathlib import Path

import nox

os.environ.update({"PDM_IGNORE_SAVED_PYTHON": "1"})
os.environ.update({"PDM_USE_VENV": "1"})

nox.options.reuse_existing_virtualenvs = True


@nox.session(python=["3.7", "3.8", "3.9", "3.10"])
def test(session):
    """Run the tests."""
    session.install("pytest")
    session.run("pytest")


@nox.session(python="3.9")
def coverage(session) -> None:
    """Upload coverage data."""
    session.install("coverage[toml]")
    session.run("pdm", "install", "-G", "dev", external=True)
    session.run("pdm", "run", "coverage", "report", "--fail-under=0", external=True)


@nox.session(python="3.9")
def docs(session):
    """Build the documentation."""
    session.run("pdm", "install", "-G", "dev", external=True)
    args = session.posargs or ["-W", "-n", "docs", "docs/_build"]

    if session.interactive and not session.posargs:
        args = ["-a", "--watch=docs/_static", "--open-browser", *args]

    builddir = Path("docs", "_build")
    if builddir.exists():
        shutil.rmtree(builddir)

    session.install("-r", "docs/requirements.txt")

    if session.interactive:
        session.run("pdm", "run", "sphinx-autobuild", *args, external=True)
    else:
        session.run("pdm", "run", "sphinx-build", *args, external=True)
