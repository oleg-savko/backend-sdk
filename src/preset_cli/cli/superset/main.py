"""
Main entry point for Superset commands.
"""
from typing import Any

import click
from preset_cli.auth.main import UsernamePasswordAuth
from preset_cli.cli.superset.export import export
from preset_cli.cli.superset.sql import sql
from preset_cli.cli.superset.sync.main import sync
from preset_cli.lib import setup_logging
from yarl import URL


@click.group()
@click.argument("instance")
@click.option("-u", "--username", default="admin", help="Username")
@click.option(
    "-p",
    "--password",
    prompt=True,
    prompt_required=False,
    default="admin",
    hide_input=True,
    help="Password (leave empty for prompt)",
)
@click.option("--loglevel", default="INFO")
@click.pass_context
def superset_cli(
        ctx: click.core.Context,
        instance: str,
        username: str = "admin",
        password: str = "admin",
        loglevel: str = "INFO"
):
    """
    An Apache Superset CLI.
    """
    ctx.ensure_object(dict)

    ctx.obj["INSTANCE"] = instance

    setup_logging(loglevel)

    # allow a custom authenticator to be passed via the context
    if "AUTH" not in ctx.obj:
        ctx.obj["AUTH"] = UsernamePasswordAuth(URL(instance), username, password)


superset_cli.add_command(sql)
superset_cli.add_command(sync)
superset_cli.add_command(export)


@click.group()
@click.pass_context
def superset(ctx: click.core.Context) -> None:
    """
    Send commands to one or more Superset instances.
    """
    ctx.ensure_object(dict)


def mutate_commands(source: click.core.Group, target: click.core.Group) -> None:
    """
    Programmatically modify commands so they work with workspaces.
    """
    for name, command in source.commands.items():

        if isinstance(command, click.core.Group):

            @click.group()
            @click.pass_context
            def new_group(
                    ctx: click.core.Context, *args: Any, command=command, **kwargs: Any
            ) -> None:
                ctx.invoke(command, *args, **kwargs)

            mutate_commands(command, new_group)
            new_group.params = command.params[:]
            target.add_command(new_group, name)

        else:

            @click.command()
            @click.pass_context
            def new_command(
                    ctx: click.core.Context, *args: Any, command=command, **kwargs: Any
            ) -> None:
                for instance in ctx.obj["WORKSPACES"]:
                    click.echo(f"\n{instance}")
                    ctx.obj["INSTANCE"] = instance
                    ctx.invoke(command, *args, **kwargs)

            new_command.params = command.params[:]
            target.add_command(new_command, name)


mutate_commands(superset_cli, superset)
