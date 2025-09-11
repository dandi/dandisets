#!/bin/bash
#
# Get all released assets by going through tags and getting their keys
#
set -eu

for ds in "$@"; do
(
	cd "$ds";
	echo "I: $ds"
	git tag | { grep -E '^[0-9]\.[0-9]{6}\.[0-9]{4}' || : ; } | sort | \
	while read -r tag; do
		# some had removed, e.g. in 000121
		case "$tag" in
			0.210815.0703|0.220124.2156) continue;;
		esac
		if ! git annex copy --branch="$tag" -J 5 --in here --not --in dandi-dandisets-dropbox --to dandi-dandisets-dropbox; then
			echo "ERROR: run on $ds for $tag failed with $?"
		fi
	done
)
done
