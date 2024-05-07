#!/bin/bash
#
# Get all released assets by going through tags and getting their keys
#
set -eu

for ds in "$@"; do
(
	cd "$ds";
	echo "I: $ds"
	git tag | grep -E '^[0-9]\.[0-9]{6}\.[0-9]{4}' | sort | \
	while read tag; do
		git annex get --branch="$tag" -J 5
	done
)
done
