from pathlib import Path
import re

import click


@click.command()
@click.argument(
    "gitmodules", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
def main(gitmodules):
    blocks = {}
    with gitmodules.open("r") as fp:
        for (name, block) in parse_gitmodules(fp):
            if name not in blocks:
                blocks[name] = block
    with gitmodules.open("w") as fp:
        for _, block in sorted(blocks.items()):
            fp.write(block)


def parse_gitmodules(fp):
    # Returns an iterator of (submodule name, submodule text) pairs
    name = None
    block = None
    for line in fp:
        if m := re.fullmatch(r'\[submodule "(.+)"\]\s*', line):
            if name is not None:
                yield (name, block)
            name = m[1]
            block = line
        else:
            assert block is not None
            block += line
    if name is not None:
        yield (name, block)


if __name__ == "__main__":
    main()
