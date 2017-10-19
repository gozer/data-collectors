from collectors.adjust import adjust_cmd
from collectors.redash import redash_cmd
from collectors.main import cli

# Add subcommands here to get around weird inheritance issues
cli.add_command(adjust_cmd)
cli.add_command(redash_cmd)

# Workaroud to pass an empty context.obj
def main():
    cli(obj={})

__all__ = ['main']

"""
Collectors provides a set of CLI tools for gathering datasets from (mainly) REST sources
"""
