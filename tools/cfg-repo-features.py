#!/usr/bin/python3
__requires__ = ["click >= 7.0", "PyGithub == 2.*"]
from subprocess import check_output

import click
from github import Auth, Github


@click.command()
@click.argument("organization")
def main(organization):
    token = check_output(["git", "config", "hub.oauthtoken"], text=True).strip()
    gh = Github(auth=Auth.Token(token))
    for repo in gh.get_organization(organization).get_repos():
        print(repo.full_name)
        repo.edit(has_issues=True, has_wiki=False)


if __name__ == "__main__":
    main()
