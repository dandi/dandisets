#!/usr/bin/env python3
__requires__ = ["PyGithub ~= 2.0"]
import logging
import subprocess
import sys

from github import Auth, Github

ORGANIZATION = "dandisets"

dandisets = sys.argv[1:]

logging.basicConfig(format="[%(levelname)-8s] %(message)s", level=logging.INFO)

log = logging.getLogger()

token = subprocess.run(
    ["git", "config", "hub.oauthtoken"],
    check=True,
    stdout=subprocess.PIPE,
    text=True,
).stdout.strip()

gh = Github(auth=Auth.Token(token))

for did in dandisets:
    log.info("Creating releases for Dandiset %s ...", did)
    repo = gh.get_repo(f"{ORGANIZATION}/{did}")
    tags = {t.name for t in repo.get_tags()}
    releases = {r.tag_name for r in repo.get_releases()}
    for tag in sorted(tags - releases):
        log.info("Creating release for tag %s", tag)
        repo.create_git_release(tag, tag, "")
