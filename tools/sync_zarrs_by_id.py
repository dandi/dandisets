from pathlib import Path
import sys

import anyio

from backups2datalad.adandi import AsyncDandiClient
from backups2datalad.config import BackupConfig, ResourceConfig
from backups2datalad.logging import log
from backups2datalad.manager import Manager
from backups2datalad.zarr import sync_zarr

import logging
logging.basicConfig(level=logging.INFO)

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing

dandiset_id = sys.argv[1]
zarrs_folder = sys.argv[2]
zarr_ids = sys.argv[3:]

print(f"Working on {len(zarr_ids)} for dandiset {dandiset_id} under {zarrs_folder}")

async def amain():
    config = BackupConfig(zarrs=ResourceConfig(path="dandizarrs"))
    async with AsyncDandiClient.for_dandi_instance("dandi") as client:
        dandiset = await client.get_dandiset(dandiset_id, "draft")
        async with aclosing(dandiset.aget_zarr_assets()) as ait:
            async for asset in ait:
                if asset.zarr in zarr_ids:
                    print(f"SYNCING {asset.zarr} ({asset.path})")
                    checksum = asset.get_digest().value
                    await sync_zarr(
                        asset,
                        checksum,
                        Path(zarrs_folder, asset.zarr),
                        Manager(config=config, gh=None, log=log),
                    )
                    zarr_ids.pop(zarr_ids.index(asset.zarr))
    if zarr_ids:
        print(
            f"Following zarr ids were not found associated with {dandiset_id} and "
            f"thus not processed: {', '.join(zarr_ids)}")


if __name__ == "__main__":
    anyio.run(amain)
