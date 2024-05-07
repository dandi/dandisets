#!/bin/bash
#
# Prototype script to add released versions into metadata, but not really needed
# since  git-annex get  also has  --branch
#
set -eu

for ds in "$@"; do
(
	cd "$ds";
	echo "I: $ds"
	git tag | grep -E '^[0-9]\.[0-9]{6}\.[0-9]{4}' | sort | \
	while read tag; do
		git annex metadata --fast --branch="$tag" --not --metadata in-release="$tag" --set in-release+="$tag"
	done
	# Now we can get data files which were referenced by any release
	# git annex get -J5 --metadata in-release=*
)
done
