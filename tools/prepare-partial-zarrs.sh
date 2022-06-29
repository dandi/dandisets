#!/bin/bash
set -eux -o pipefail

backup_root=/mnt/backup/dandi
dandiset_root="$backup_root/dandisets"
zarr_root="$backup_root/dandizarrs"
partial_zarrs="$backup_root/partial-zarrs"

dandiset=000108

zarr_info_file="$(mktemp)"
git_status_file="$(mktemp)"

mkdir -p "$partial_zarrs"

export GIT_AUTHOR_NAME="DANDI User"
export GIT_AUTHOR_EMAIL="info@dandiarchive.org"

cd "$zarr_root"
for zarr in *-*-*-*-*
do
    echo "[INFO] Checking Zarr $zarr ..."
    curl -fsSL -o "$zarr_info_file" "https://api.dandiarchive.org/api/zarr/$zarr/"
    zarr_dandiset="$(jq -r .dandiset "$zarr_info_file")"
    path="$(jq -r .name "$zarr_info_file")"
    if [[ "$zarr_dandiset" != "$dandiset" ]]
    then
        echo "[INFO] Zarr is for different Dandiset; skipping"
        continue
    fi
    if [ -e "$dandiset_root/$dandiset/$path" ]
    then
        echo "[INFO] Zarr is already in Dandiset dataset; skipping"
        continue
    fi
    cd "$zarr_root/$zarr"
    if [ ! -e .dandi/zarr-checksum ]
    then
        echo "[INFO] Zarr backup is not complete; resetting"
        git reset --hard HEAD
        git clean -dxf
        echo "[INFO] Moving Zarr to partial-zarrs dir"
        cd ..
        mv "$zarr" "$partial_zarrs"
    else
        git status --porcelain --branch > "$git_status_file"
        if head -n1 "$git_status_file" | grep -Fwq gone
        then
            echo "[INFO] Zarr was never pushed to GitHub; pushing"
            git push -u github draft
        fi
        if grep -vq '^#' "$git_status_file"
        then
            echo "[INFO] Zarr backup was not saved; resetting"
            git reset --hard HEAD
            git clean -dxf
            echo "[INFO] Moving Zarr to partial-zarrs dir"
            cd ..
            mv "$zarr" "$partial_zarrs"
        else
            echo "[INFO] Zarr is backed up; adding to Dandiset dataset"
            cd "$dandiset_root/$dandiset"
            datalad clone https://github.com/dandizarrs/"$zarr" "$path"
            cd "$path"
            git remote rename origin github
            datalad_id="$(git config --file .datalad/config --get datalad.dataset.id)"
            commit_date="$(git show -s --format='%ai')"
            cd -
            git submodule add https://github.com/dandizarrs/"$zarr" "$path"
            git config \
                --file .gitmodules \
                --replace-all \
                submodule."$path".datalad-id \
                "$datalad_id"
            GIT_AUTHOR_DATE="$commit_date" datalad save \
                -m "[backups2datalad] Backed up Zarr $zarr to $path" \
                "$path" .gitmodules
        fi
    fi
done
