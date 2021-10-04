import nox

nox.options.reuse_existing_virtualenvs = True


@nox.session
def test(session):
    session.install("-r", "backups2datalad.req.txt")
    session.install("pytest")
    session.install("datalad[tests]")
    session.run("pytest", *session.posargs, "test_backups2datalad.py")


@nox.session
def typing(session):
    session.install("-r", "backups2datalad.req.txt")
    session.install("-r", "get-upload-stats.req.txt")
    session.install("mypy", "boto3-stubs[s3]", "types-python-dateutil")
    session.run(
        "mypy", "backups2datalad.py", "test_backups2datalad.py", "get-upload-stats.py"
    )
