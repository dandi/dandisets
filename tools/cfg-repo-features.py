#!/usr/bin/python3
__requires__ = ["click == 7.*", "PyGithub == 1.*"]
from subprocess import check_output

import click
from github import Github


@click.command()
@click.argument("organization")
def main(organization):
    token = check_output(
        ["git", "config", "hub.oauthtoken"], universal_newlines=True
    ).strip()
    gh = Github(token)
    for repo in gh.get_organization(organization).get_repos():
        print(repo.full_name)
        repo.edit(has_issues=True, has_wiki=False)


if __name__ == "__main__":
    main()
