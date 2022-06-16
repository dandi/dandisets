import platform

import httpx

DEFAULT_BRANCH = "draft"

DEFAULT_GIT_ANNEX_JOBS = 10

DEFAULT_WORKERS = 5

# Maximum number of Zarrs to process at once
ZARR_LIMIT = 32

USER_AGENT = (
    "backups2datalad (https://github.com/dandi/dandisets) httpx/{} {}/{}".format(
        httpx.__version__,
        platform.python_implementation(),
        platform.python_version(),
    )
)
