from pathlib import Path
import anyio
from backups2datalad.adandi import AsyncDandiClient
from backups2datalad.config import BackupConfig, ResourceConfig
from backups2datalad.manager import Manager
from backups2datalad.zarr import sync_zarr
from backups2datalad.logging import log

import sys

dandiset_id = sys.argv[1]
paths = sys.argv[2:]

async def amain():
    async with AsyncDandiClient.for_dandi_instance("dandi") as client:
        dandiset = await client.get_dandiset(dandiset_id, "draft")
        for path in paths:
            asset = await dandiset.aget_asset_by_path(path)
            print(f"{path} {asset.zarr}")

if __name__ == "__main__":
    anyio.run(amain)
