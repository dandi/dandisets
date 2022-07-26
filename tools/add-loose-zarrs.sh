#!/bin/bash
set -eux -o pipefail

backup_root=/mnt/backup/dandi
dandiset_root="$backup_root/dandisets"
dandiset=000108

while read path
do
    cd "$dandiset_root/$dandiset/$path"
    if git remote | grep -Fqx origin
    then git remote rename origin github
    fi
    #datalad_id="$(git config --file .datalad/config --get datalad.dataset.id)"
    commit_date="$(git show -s --format='%ai')"
    url="$(git remote get-url github)"
    zarr="$(basename "$url")"
    cd "$dandiset_root/$dandiset"
    #git submodule add "$url" "$path"
    #git config \
    #    --file .gitmodules \
    #    --replace-all \
    #    submodule."$path".datalad-id \
    #    "$datalad_id"
    GIT_AUTHOR_DATE="$commit_date" datalad save -d . \
        -m "[backups2datalad] Backed up Zarr $zarr to $path" \
        "$path"
done
