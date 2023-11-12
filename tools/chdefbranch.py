#!/usr/bin/python3
__requires__ = ["click >= 8.0.1", "PyGithub ~= 2.0"]
from pathlib import Path
from subprocess import check_output, run

import click
from github import Auth, Github


@click.command()
@click.argument("organization")
@click.argument(
    "datasets", type=click.Path(file_okay=False, exists=True, path_type=Path), nargs=-1
)
def main(organization, datasets):
    """
    Change default branch from 'master' to 'draft'.

    Run as ``python chdefbranch.py ORG DATASET ...`` where ``ORG`` is the name
    of the GitHub organization to which the GitHub repositories belong and each
    ``DATASET`` is a path to a local clone of a repository to update.
    """
    token = check_output(["git", "config", "hub.oauthtoken"], text=True).strip()
    gh = Github(auth=Auth.Token(token))
    org = gh.get_organization(organization)
    for ds in datasets:
        repo = org.get_repo(ds.name)
        print(repo.full_name)
        # <https://stackoverflow.com/a/55020304/744178>
        master = repo.get_git_ref("heads/master")
        repo.create_git_ref("refs/heads/draft", sha=master.object.sha)
        repo.edit(default_branch="draft")
        master.delete()

        def git(*args):
            run(["git", *args], cwd=ds, check=True)  # noqa: B023

        git("branch", "-m", "master", "draft")
        git("fetch", "github")
        git("branch", "-u", "github/draft", "draft")
        git("remote", "set-head", "github", "-a")
        git("remote", "prune", "github")


if __name__ == "__main__":
    main()
