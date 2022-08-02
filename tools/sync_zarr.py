from pathlib import Path
import anyio
from backups2datalad.adandi import AsyncDandiClient
from backups2datalad.config import BackupConfig, ResourceConfig
from backups2datalad.manager import Manager
from backups2datalad.zarr import sync_zarr
from backups2datalad.logging import log

dandiset_id = "FILL IN HERE"
zarr_asset_path = "FILL IN HERE"
backup_folder = "FILL IN HERE"

async def amain():
    async with AsyncDandiClient.for_dandi_instance("dandi") as client:
        dandiset = await client.get_dandiset(dandiset_id, "draft")
        asset = await dandiset.aget_asset_by_path(zarr_asset_path)
        checksum = asset.get_digest().value
        config = BackupConfig(zarrs=ResourceConfig(path="zarrs"))
        await sync_zarr(
            asset,
            checksum,
            Path(backup_folder, asset.zarr),
            Manager(config=config, gh=None, log=log)
        )

if __name__ == "__main__":
    anyio.run(amain)
