import os
import shutil
from pathlib import Path

import nox

nox.options.reuse_existing_virtualenvs = True


@nox.session(python=["3.7", "3.8", "3.9", "3.10"])
def test(session):
    """Run the tests."""
    session.install(".")
    session.install("pytest")
    session.run("pytest")


@nox.session(python="3.10")
def coverage(session) -> None:
    """Upload coverage data."""
    session.install(".")
    session.install("coverage[toml]")
    session.run("coverage", "report", "--fail-under=0")


@nox.session(python="3.10")
def docs(session):
    """Build the documentation."""
    session.install(".")
    session.run("sphinx-build", "docs", "docs/_build")

    # args = session.posargs or ["-W", "-n", "docs", "docs/_build"]

    # if session.interactive and not session.posargs:
    #     args = ["-a", "--watch=docs/_static", "--open-browser", *args]

    # builddir = Path("docs", "_build")
    # if builddir.exists():
    #     shutil.rmtree(builddir)

    # session.install("-r", "docs/requirements.txt")

    # if session.interactive:
    #     session.run("sphinx-autobuild", *args, external=True)
    # else:
    #     session.run("sphinx-build", *args, external=True)
