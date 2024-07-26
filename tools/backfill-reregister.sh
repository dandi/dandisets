#!/bin/bash
# <https://github.com/dandi/backups2datalad/pull/36#issuecomment-2153627640>
set -eux -o pipefail

dandiset_root=/mnt/backup/dandi/dandisets

cd "$dandiset_root"
for ds in 0*
do
    cd "$ds"
    embargo_status="$(git config --file .datalad/config --default OPEN --get dandi.dandiset.embargo-status)"
    if [ "$embargo_status" = OPEN ] \
        && git remote | grep -Fqx datalad \
        && git log -S EMBARGOED -n1 -- .datalad/config | grep -q .
    then
        git annex find --include='*' --format='${key}\n' \
            | git annex reregisterurl --batch --move-from datalad
        git remote remove datalad
    fi
    cd -
done
