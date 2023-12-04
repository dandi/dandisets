This repository is a [Datalad](https://www.datalad.org) super-dataset
containing Datalad datasets mirroring each of the Dandisets on
<https://dandiarchive.org>.  The mirrors are updated by a cronjob on an hourly
basis.

The repository and all of the Dandisets within can be cloned by running:

    datalad install -r https://github.com/dandi/dandisets

See <https://handbook.datalad.org> and <https://www.dandiarchive.org/handbook>
for more information.

Running the Mirroring Command
=============================

Creating & updating dataset mirrors of Dandisets is done with the
`backups2datalad` command located in `tools/`.

Setup
-----

Before running `backups2datalad`, the following setup must be performed:

- A Python environment must be set up with the packages listed in
  `tools/backups2datalad.req.txt` installed.  The minimum required Python
  version is given in a comment at the top of the file.

- [git-annex](https://git-annex.branchable.com) must be installed.  The minimum
  required version is given in a comment near the top of
  `tools/backups2datalad.req.txt`.

- A configuration file should be written.  This is a YAML file containing a
  mapping with the following keys:

    - `dandi_instance` — The name of the [DANDI
      instance](https://dandi.readthedocs.io/en/latest/cmdline/instances.html)
      whose Dandisets should be mirrored.  Defaults to `"dandi"`.

    - `s3bucket` — The name of the S3 bucket on which the assets for the DANDI
      instance are stored.  Currently, only buckets in the `us-east-1` region
      are supported.  Defaults to `"dandiarchive"`.

        - When `dandi_instance` is `"dandi"`, this should be `"dandiarchive"`.

        - When `dandi_instance` is `"dandi-staging"`, this should be
          `"dandi-api-staging-dandisets"`.

    - `content_url_regex` — A regular expression used to identify which of an
      asset's `contentUrl`s is its S3 URL.  Defaults to
      `"amazonaws.com/.*blobs/"`.

    - `dandisets` — A mapping containing configuration specific to the
      mirroring of Dandisets.  If not given, it will default to a mapping in
      which `path` is set to `"dandisets"` and all other fields are unset.

        - `path` *(required)* — The path to the local directory in which
          dataset mirrors of Dandisets will be placed, relative to
          `backup_root`.  The directory need not already exist.

            - This directory will be made into a DataLad dataset.

        - `github_org` — The name of the GitHub organization (which must
          already exist) to which the mirror repositories will be pushed.  If
          not set, mirrors will not be pushed to GitHub.

            - `dandisets.github_org` and `zarrs.github_org` must be either both
              set or both unset.

        - `remote` — Description of a git-annex special remote to create in new
          mirror repositories and for the `populate` subcommand to copy data
          to.  If not set, `populate` cannot be run.

          When present, `remote` is a mapping with the following keys:

            - `name` *(required)* — The name of the remote
            - `type` *(required)* — The type of the remote
            - `options` *(required)* — A string-valued mapping specifying
              parameters to pass to `git-annex initremote`

    - `zarrs` — A mapping containing configuration specific to the mirroring of
      Zarrs.  If not given, `backups2datalad` will error upon trying to back up
      a Dandiset containing a Zarr.  The mapping has the same schema as for
      `dandisets`.

        - `zarrs.path` will not be made into a DataLad dataset.

        - `dandisets.github_org` and `zarrs.github_org` must be either both set
          or both unset.

        - `zarrs.remote` is a prerequisite for the `populate-zarrs` subcommand.

    - `backup_root` — The path to the local directory in which the Dandiset and
      Zarr mirror directories will be placed.  Defaults to the current
      directory.

        - This option can also be set via the `--backup-root` global CLI
          option, which overrides any value given in the configuration file.

    - `asset_filter` — A regular expression; if given, only assets whose paths
      match the regex will be processed.

        - This option can also be set via the `--asset-filter` option of the
          `update-from-backup` and `release` subcommands, which overrides any
          value given in the configuration file.

    - `jobs` *(integer)* — The number of parallel git-annex jobs to use when
      downloading & pushing assets.  Defaults to 10.

        - This option can also be set via the `--jobs` global CLI option, which
          overrides any value given in the configuration file.

    - `workers` *(integer)* — The number of asynchronous worker tasks to run
      concurrently.  Defaults to 5.

        - This option can also be set via the `--workers` option of the
          `update-from-backup`, `backup-zarrs`, `populate`, and
          `populate-zarrs` subcommands, which overrides any value given in the
          configuration file.

    - `force` — If set to `"assets-update"`, all assets are forcibly updated,
      even those whose metadata hasn't changed.

        - This option can also be set via the `--force` option of the
          `update-from-backup` and `release` subcommands, which overrides any
          value given in the configuration file.

    - `enable_tags` *(boolean)* — Whether to enable creation of tags for
      releases; defaults to `true`

        - This option can also be set via the `--tags`/`--no-tags` options of
          the `update-from-backup` subcommand, which override any value given
          in the configuration file.

    - `gc_assets` *(boolean)* — If set and `assets.json` contains any assets
      neither on the server nor in the backup, delete the extra assets instead
      of erroring.  Defaults to `false`.

        - This option can also be set via the `--gc-assets` option of the
          `update-from-backup` subcommand, which overrides any value given in
          the configuration file.

    - `mode` — Specify how to decide whether to back up a Dandiset.  Possible
      values are:

        - `"timestamp"` *(default)* — only back up if the timestamp of the last
          backup is older than the "modified" timestamp on the server

        - `"force"` — always back up

        - `"verify"`  — always back up, but error if there are any changes
          without a change to the "modified" timestamp

      This option can also be set via the `--mode` option of the
      `update-from-backup` subcommand, which overrides any value given in the
      configuration file.

    - `zarr_mode` — Specify how to decide whether to back up a Zarr.  Possible
      values are:

        - `"timestamp"` *(default)* — only back up if the timestamp of the last
          backup is older than some Zarr entry in S3

        - `"checksum"` — only back up if the Zarr checksum is out of date or
          doesn't match the expected value

        - `"asset-checksum"` — only back up if the Zarr asset's "modified"
          timestamp is later than that in `assets.json` and the checksum is out
          of date or doesn't match the expected value

        - `"force"` — always back up

      This option can also be set via the `--zarr-mode` option of the
      `update-from-backup` subcommand, which overrides any value given in the
      configuration file.

- If pushing mirror repositories to GitHub, a GitHub access token with
  appropriate permissions must be stored in the `hub.oauthtoken` key of your
  `~/.gitconfig`, and an SSH key that has been registered with a GitHub account
  must be in use as well.

Usage
-----

Run `backups2datalad` from the `tools/` directory with:

    python3 -m backups2datalad --config path/to/config/file <subcommand> ...

Run `python3 -m backups2datalad --help` for details on the global options and
summaries of the subcommands.

`backups2datalad` subcommands:

- `update-from-backup` — Create & update local mirrors of Dandisets and the
  Zarrs within them

- `backup-zarrs` — Create (but do not update) local mirrors of Zarrs for a
  single Dandiset

- `update-github-metadata` — Update homepages and descriptions for mirrors
  pushed to GitHub

- `release` — Create a tag (and a GitHub release, if pushing to GitHub) in a
  Dandiset mirror for a given published version

- `populate` — Copy assets from local Dandiset mirrors to the git-annex special
  remote

- `populate-zarrs` — Copy assets from local Zarr mirrors to the git-annex
  special remote

- `zarr-checksum` — Computes the Zarr checksum for a given Zarr mirror

Run `python3 -m backups2datalad <subcommand> --help` for further details on
each subcommand.

The primary mirroring subcommands are `update-from-backup`, `populate`, and
`populate-zarrs`; the other subcommands are for minor/maintenance tasks and
usually do not need to be run.
