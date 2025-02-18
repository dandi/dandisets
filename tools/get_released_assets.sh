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
		# some had removed, e.g. in 000121
		case "$tag" in
			0.210815.0703|0.220124.2156) continue;;
		esac
		git annex get --branch="$tag" -J 5 --from=web
	done
)
done
