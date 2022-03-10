import nox

nox.options.reuse_existing_virtualenvs = True
nox.options.stop_on_first_error = True


@nox.session
def typing(session):
    session.install("-r", "backups2datalad.req.txt")
    session.install("-r", "get-upload-stats.req.txt")
    session.install(
        "mypy", "boto3-stubs[s3]", "trio-typing[mypy]", "types-python-dateutil"
    )
    session.run(
        "mypy", "backups2datalad", "test_backups2datalad", "get-upload-stats.py"
    )


@nox.session
def test(session):
    session.install("-r", "backups2datalad.req.txt")
    session.install("pytest", "pytest-cov", "datalad[tests]", "zarr")
    session.run("python", "-m", "pytest", *session.posargs, "test_backups2datalad")
