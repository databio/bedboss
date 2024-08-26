import typer
from bedboss._version import __version__

app_bbuploader = typer.Typer(
    pretty_exceptions_short=False,
    pretty_exceptions_show_locals=False,
    help="Automatic BEDbase uploader for GEO data",
)


@app_bbuploader.command(
    help="Run bedboss uploading pipeline for specified genome in specified period of time."
)
def upload_all(
    bedbase_config: str = typer.Option(..., help="Path to bedbase config file"),
    outfolder: str = typer.Option(..., help="Path to output folder"),
    start_date: str = typer.Option(
        None, help="The earliest date when opep was updated [Default: 2000/01/01]"
    ),
    end_date: str = typer.Option(
        None, help="The latest date when opep was updated [Default: today's date]"
    ),
    search_limit: int = typer.Option(
        10, help="Limit of projects to be searched. [Default: 10]"
    ),
    search_offset: int = typer.Option(
        0, help="Limit of projects to be searched. [Default: 0]"
    ),
    download_limit: int = typer.Option(
        100, help="Limit of projects to be downloaded [Default: 100]"
    ),
    genome: str = typer.Option(
        None,
        help="Reference genome [Default: None] (e.g. hg38) - if None, all genomes will be processed",
    ),
    create_bedset: bool = typer.Option(
        True, help="Create bedset from bed files. [Default: True]"
    ),
    rerun: bool = typer.Option(True, help="Re-run all the samples. [Default: False]"),
    run_skipped: bool = typer.Option(
        True, help="Run skipped projects. [Default: False]"
    ),
    run_failed: bool = typer.Option(True, help="Run failed projects. [Default: False]"),
):
    from .main import upload_all as upload_all_function

    upload_all_function(
        bedbase_config=bedbase_config,
        outfolder=outfolder,
        start_date=start_date,
        end_date=end_date,
        search_limit=search_limit,
        search_offset=search_offset,
        download_limit=download_limit,
        genome=genome,
        create_bedset=create_bedset,
        rerun=rerun,
        run_skipped=run_skipped,
        run_failed=run_failed,
    )


@app_bbuploader.command(help="Run bedboss uploading pipeline for GSE.")
def upload_gse(
    bedbase_config: str = typer.Option(..., help="Path to bedbase config file"),
    outfolder: str = typer.Option(..., help="Path to output folder"),
    gse: str = typer.Option(
        ..., help="GSE number that can be found in pephub. eg. GSE123456"
    ),
    create_bedset: bool = typer.Option(
        True, help="Create bedset from bed files. [Default: True]"
    ),
    genome: str = typer.Option(
        None,
        help=" reference genome to upload to database. If None, all genomes will be processed",
    ),
    rerun: bool = typer.Option(True, help="Re-run all the samples. [Default: False]"),
    run_skipped: bool = typer.Option(
        True, help="Run skipped projects. [Default: False]"
    ),
    run_failed: bool = typer.Option(True, help="Run failed projects. [Default: False]"),
):
    from .main import upload_gse as upload_gse_function

    upload_gse_function(
        bedbase_config=bedbase_config,
        outfolder=outfolder,
        gse=gse,
        create_bedset=create_bedset,
        genome=genome,
        rerun=rerun,
        run_skipped=run_skipped,
        run_failed=run_failed,
    )


def version_callback(value: bool):
    if value:
        typer.echo(f"Bedboss version: {__version__}")
        raise typer.Exit()


@app_bbuploader.callback()
def common(
    ctx: typer.Context,
    version: bool = typer.Option(
        None, "--version", "-v", callback=version_callback, help="App version"
    ),
):
    pass
