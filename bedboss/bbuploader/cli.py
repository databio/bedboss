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
        50, help="Limit of projects to be searched. [Default: 50]"
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
    preload: bool = typer.Option(
        True, help="Download bedfile before caching it. [Default: True]"
    ),
    create_bedset: bool = typer.Option(
        True, help="Create bedset from bed files. [Default: True]"
    ),
    overwrite: bool = typer.Option(
        False, help="Overwrite existing bedfiles. [Default: False]"
    ),
    overwrite_bedset: bool = typer.Option(
        True, help="Overwrite existing bedset. [Default: False]"
    ),
    rerun: bool = typer.Option(False, help="Re-run all the samples. [Default: False]"),
    run_skipped: bool = typer.Option(
        True, help="Run skipped projects. [Default: False]"
    ),
    run_failed: bool = typer.Option(True, help="Run failed projects. [Default: False]"),
    standardize_pep: bool = typer.Option(
        False, help="Standardize pep with BEDMESS. [Default: False]"
    ),
    use_skipper: bool = typer.Option(
        False,
        help="Use skipper to skip projects if they were processed locally [Default: False]",
    ),
    reinit_skipper: bool = typer.Option(
        False, help="Reinitialize skipper. [Default: False]"
    ),
    lite: bool = typer.Option(
        False, help="Run the pipeline in lite mode. [Default: False]"
    ),
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
        preload=preload,
        rerun=rerun,
        run_skipped=run_skipped,
        run_failed=run_failed,
        standardize_pep=standardize_pep,
        use_skipper=use_skipper,
        reinit_skipper=reinit_skipper,
        overwrite=overwrite,
        overwrite_bedset=overwrite_bedset,
        lite=lite,
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
    preload: bool = typer.Option(
        True, help="Download bedfile before caching it. [Default: True]"
    ),
    rerun: bool = typer.Option(True, help="Re-run all the samples. [Default: False]"),
    run_skipped: bool = typer.Option(
        True, help="Run skipped projects. [Default: False]"
    ),
    run_failed: bool = typer.Option(True, help="Run failed projects. [Default: False]"),
    overwrite: bool = typer.Option(
        False, help="Overwrite existing bedfiles. [Default: False]"
    ),
    overwrite_bedset: bool = typer.Option(
        True, help="Overwrite existing bedset. [Default: False]"
    ),
    standardize_pep: bool = typer.Option(
        False, help="Standardize pep with BEDMESS. [Default: False]"
    ),
    use_skipper: bool = typer.Option(
        False,
        help="Use local skipper to skip projects if they were processed locally [Default: False]",
    ),
    reinit_skipper: bool = typer.Option(
        False, help="Reinitialize skipper. [Default: False]"
    ),
    lite: bool = typer.Option(
        False, help="Run the pipeline in lite mode. [Default: False]"
    ),
):
    from .main import upload_gse as upload_gse_function

    upload_gse_function(
        bedbase_config=bedbase_config,
        outfolder=outfolder,
        gse=gse,
        create_bedset=create_bedset,
        genome=genome,
        preload=preload,
        rerun=rerun,
        run_skipped=run_skipped,
        run_failed=run_failed,
        standardize_pep=standardize_pep,
        use_skipper=use_skipper,
        reinit_skipper=reinit_skipper,
        overwrite=overwrite,
        overwrite_bedset=overwrite_bedset,
        lite=lite,
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
