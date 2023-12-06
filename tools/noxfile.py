import nox

nox.options.reuse_existing_virtualenvs = True
nox.options.stop_on_first_error = True


@nox.session
def typing(session):
    session.install("-r", "get-upload-stats.req.txt")
    session.install("mypy", "types-python-dateutil")
    session.run("mypy", "get-upload-stats.py")
