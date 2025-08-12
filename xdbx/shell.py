#!/usr/bin/env python3

import click
import click_shell
from .database import Database
from .transaction import Transaction


def cleanup(ctx):
    click.echo("Autoclean...")
    if 'db_store' in ctx.obj:
        for x in ctx.obj['db_store']:
            click.echo(f"Cleaning {x}")
            if isinstance(ctx.obj['db_store'][x], Database):
                ctx.obj['db_store'][x].close()
    click.echo("Goodbye!")

@click_shell.shell(
    prompt='dbx> ',
    intro="XDBX Management Shell v1.0",
    on_finished=cleanup
)   
@click.pass_context
def cli(ctx):
    ctx.obj = {}


@cli.command(
    'create',
    short_help="Create/Open a database"
)
@click.option(
    '-a', '--autocommit',
    is_flag=True,
    help="Autocommit Database",
    show_default=True,
    default=True
)
@click.option(
    '-m', '--memory',
    is_flag=True,
    help="Is it a memory Database",
    show_default=True,
    default=False
)
@click.argument(
    'db',
    type=str
)
@click.pass_context
def create(ctx, db: str, autocommit: bool, memory: bool):
    # Initialize db store if not already done
    if 'db_store' not in ctx.obj:
        ctx.obj['db_store'] = {}

    if db in ctx.obj['db_store']:
        click.echo(f"Database '{db}' is already open.")
    else:
        if memory:
            ctx.obj['db_store'][db] = Database(':memory:', autocommit=autocommit)
        else:
            ctx.obj['db_store'][db] = Database(f'{db}.db', autocommit=autocommit)
        click.echo(f"Opened database '{db}'.")


@cli.command(
    'close',
    short_help="Close an open database"
)
@click.argument(
    'db',
    type=str,
    default=''
)
@click.pass_context
def close(ctx, db: str):
    if db == '' and 'db_store' in ctx.obj:
        for x in ctx.obj['db_store']:
            ctx.obj['db_store'][x].close()    
    elif 'db_store' not in ctx.obj or db not in ctx.obj['db_store']:
        click.echo(f"No open database found with name '{db}'.")
    else:
        ctx.obj['db_store'][db].close()
        del ctx.obj['db_store'][db]
        click.echo(f"Closed database '{db}'.")

@cli.command(
    'get',
    short_help="Get from DB Storage"
)
@click.option(
    '--db',
    help="Database",
    show_default=True,
    default=''
)
@click.option(
    '--store',
    help="Storage",
    show_default=True,
    default=''
)
@click.argument(
    'query',
    type=str
)
@click.pass_context
def get(ctx, db, store, query):
    if not ctx.obj['db_store'] or len(ctx.obj['db_store']) == 0:
        click.echo("No Open Databases")
    else:
        tab = {}
        if db == '':
            for db_ in ctx.obj['db_store']:
                store_ = {}
                if store == '':
                    for y in ctx.obj['db_store'][db_].storages:
                        store_[y] = ctx.obj['db_store'][db_][y, 'json'].get_path(query, None)
                    tab[db_] = store_
                else:
                    tab[db_] = ctx.obj['db_store'][db_][store, 'json'].get_path(query, None)
        elif db in ctx.obj['db_store']:
            store_ = {}
            if store == '':
                for y in ctx.obj['db_store'][db].storages:
                    store_[y] = ctx.obj['db_store'][db][y, 'json'].get_path(query, None)
                tab[db] = store_
            else:
                tab[db] = ctx.obj['db_store'][db][store, 'json'].get_path(query, None)

        click.echo(tab)

