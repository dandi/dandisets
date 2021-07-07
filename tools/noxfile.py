import nox

nox.options.reuse_existing_virtualenvs = True

@nox.session
def typing(session):
    session.install('-r', 'backups2datalad.req.txt')
    session.install("mypy", "boto3-stubs[s3]")
    session.run('mypy', 'backups2datalad.py')
