import os
from typing import Union

import typer

from bedboss import __version__
from bedboss.bbuploader.cli import app_bbuploader
from pephubclient.helpers import MessageHandler as printm

# commented and made new const here, because it speeds up help function,
# from bbconf.const import DEFAULT_LICENSE
DEFAULT_LICENSE = "DUO:0000042"

app = typer.Typer(pretty_exceptions_short=False, pretty_exceptions_show_locals=False)


def create_pm(
    outfolder: str,
    multi: bool = False,
    recover: bool = True,
    dirty: bool = False,
    pipeline_name: str = "bedboss-pipeline",
):
    import pypiper

    pm_out_folder = outfolder
    pm_out_folder = os.path.join(pm_out_folder, "pipeline_manager")

    pm: pypiper.PipelineManager = pypiper.PipelineManager(
        name=pipeline_name,
        outfolder=pm_out_folder,
        version=__version__,
        multi=multi,
        recover=recover,
        dirty=dirty,
    )
    return pm


options_list = ["bigwig", "bedgraph", "bed", "bigbed", "wig"]


def validate_input_options(option: str):
    if option not in options_list:
        raise typer.BadParameter(
            f"Invalid input type option '{option}'. Options are: {', '.join(options_list)}"
        )
    return option


@app.command(help="Run all the bedboss pipeline for a single bed file")
def run_all(
    input_file: str = typer.Option(
        ...,
        help="Path to the input file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    input_type: str = typer.Option(
        ...,
        help=f"Type of the input file. Options are: {', '.join(options_list)}",
        callback=validate_input_options,
        case_sensitive=False,
    ),
    outfolder: str = typer.Option(..., help="Path to the output folder"),
    genome: str = typer.Option(..., help="Genome name. Example: 'hg38'"),
    bedbase_config: str = typer.Option(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    license_id: str = typer.Option(
        DEFAULT_LICENSE,
        help="License ID. If not provided for in PEP"
        "for each bed file, this license will be used",
    ),
    rfg_config: str = typer.Option(None, help="Path to the rfg config file"),
    narrowpeak: bool = typer.Option(False, help="Is the input file a narrowpeak file?"),
    check_qc: bool = typer.Option(True, help="Check the quality of the input file?"),
    chrom_sizes: str = typer.Option(None, help="Path to the chrom sizes file"),
    open_signal_matrix: str = typer.Option(
        None, help="Path to the open signal matrix file"
    ),
    ensdb: str = typer.Option(None, help="Path to the EnsDb database file"),
    just_db_commit: bool = typer.Option(False, help="Just commit to the database?"),
    force_overwrite: bool = typer.Option(
        False, help="Force overwrite the output files"
    ),
    update: bool = typer.Option(
        False,
        help="Update the bedbase database with the new record if it exists. This overwrites 'force_overwrite' option",
    ),
    lite: bool = typer.Option(
        False, help="Run the pipeline in lite mode. [Default: False]"
    ),
    upload_qdrant: bool = typer.Option(False, help="Upload to Qdrant"),
    upload_s3: bool = typer.Option(False, help="Upload to S3"),
    upload_pephub: bool = typer.Option(False, help="Upload to PEPHub"),
    # Universes
    universe: bool = typer.Option(False, help="Create a universe"),
    universe_method: str = typer.Option(
        None, help="Method used to create the universe"
    ),
    universe_bedset: str = typer.Option(
        None, help="Bedset used used to create the universe"
    ),
    # PipelineManager
    multi: bool = typer.Option(False, help="Run multiple samples"),
    recover: bool = typer.Option(True, help="Recover from previous run"),
    dirty: bool = typer.Option(False, help="Run without removing existing files"),
):
    """

    Run the bedboss pipeline for a single bed file
    """
    from bbconf.bbagent import BedBaseAgent

    from bedboss.bedboss import run_all as run_all_bedboss

    agent = BedBaseAgent(bedbase_config)

    run_all_bedboss(
        input_file=input_file,
        input_type=input_type,
        outfolder=outfolder,
        genome=genome,
        bedbase_config=agent,
        license_id=license_id,
        rfg_config=rfg_config,
        narrowpeak=narrowpeak,
        check_qc=check_qc,
        chrom_sizes=chrom_sizes,
        open_signal_matrix=open_signal_matrix,
        ensdb=ensdb,
        other_metadata=None,
        lite=lite,
        just_db_commit=just_db_commit,
        force_overwrite=force_overwrite,
        update=update,
        upload_qdrant=upload_qdrant,
        upload_s3=upload_s3,
        upload_pephub=upload_pephub,
        universe=universe,
        universe_method=universe_method,
        universe_bedset=universe_bedset,
        pm=create_pm(outfolder=outfolder, multi=multi, recover=recover, dirty=dirty),
    )


@app.command(help="Run the all bedboss pipeline for a bed files in a PEP")
def run_pep(
    pep: str = typer.Option(..., help="PEP file. Local or remote path"),
    outfolder: str = typer.Option(..., help="Path to the output folder"),
    bedbase_config: str = typer.Option(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    create_bedset: bool = typer.Option(True, help="Create a new bedset"),
    bedset_heavy: bool = typer.Option(
        False, help="Run the heavy version of the bedbuncher pipeline"
    ),
    bedset_id: Union[str, None] = typer.Option(None, help="Bedset ID"),
    rfg_config: str = typer.Option(None, help="Path to the rfg config file"),
    check_qc: bool = typer.Option(True, help="Check the quality of the input file?"),
    ensdb: str = typer.Option(None, help="Path to the EnsDb database file"),
    just_db_commit: bool = typer.Option(False, help="Just commit to the database?"),
    force_overwrite: bool = typer.Option(
        False, help="Force overwrite the output files"
    ),
    update: bool = typer.Option(
        False,
        help="Update the bedbase database with the new record if it exists. This overwrites 'force_overwrite' option",
    ),
    upload_qdrant: bool = typer.Option(True, help="Upload to Qdrant"),
    upload_s3: bool = typer.Option(True, help="Upload to S3"),
    upload_pephub: bool = typer.Option(True, help="Upload to PEPHub"),
    no_fail: bool = typer.Option(False, help="Do not fail on error"),
    license_id: str = typer.Option(DEFAULT_LICENSE, help="License ID"),
    standardize_pep: bool = typer.Option(False, help="Standardize the PEP using bedMS"),
    lite: bool = typer.Option(
        False, help="Run the pipeline in lite mode. [Default: False]"
    ),
    rerun: bool = typer.Option(False, help="Rerun already processed samples"),
    # PipelineManager
    multi: bool = typer.Option(False, help="Run multiple samples"),
    recover: bool = typer.Option(True, help="Recover from previous run"),
    dirty: bool = typer.Option(False, help="Run without removing existing files"),
):
    """
    Run the bedboss pipeline for a bed files in a PEP
    """
    from bedboss.bedboss import insert_pep

    pm = create_pm(
        outfolder=outfolder,
        multi=multi,
        recover=recover,
        dirty=dirty,
        pipeline_name=pep.replace("/", "_").replace(":", "_"),
    )

    insert_pep(
        bedbase_config=bedbase_config,
        output_folder=outfolder,
        pep=pep,
        bedset_id=bedset_id,
        rfg_config=rfg_config,
        create_bedset=create_bedset,
        bedset_heavy=bedset_heavy,
        check_qc=check_qc,
        ensdb=ensdb,
        just_db_commit=just_db_commit,
        force_overwrite=force_overwrite,
        update=update,
        license_id=license_id,
        upload_s3=upload_s3,
        upload_pephub=upload_pephub,
        upload_qdrant=upload_qdrant,
        no_fail=no_fail,
        standardize_pep=standardize_pep,
        lite=lite,
        rerun=rerun,
        pm=pm,
    )

    pm.stop_pipeline()


@app.command(
    help="Run unprocessed files or reprocess them. Currently, only hg38, hg19, and mm10 genomes are supported."
)
def reprocess_all(
    bedbase_config: str = typer.Option(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    outfolder: str = typer.Option(..., help="Path to the output folder"),
    limit: int = typer.Option(100, help="Limit the number of files to reprocess"),
    no_fail: bool = typer.Option(True, help="Do not fail on error"),
):
    from bedboss.bedboss import reprocess_all as reprocess_all_function

    reprocess_all_function(
        bedbase_config=bedbase_config,
        output_folder=outfolder,
        limit=limit,
        no_fail=no_fail,
    )


@app.command(help="Run unprocessed file, or reprocess it [Only 1 file]")
def reprocess_one(
    bedbase_config: str = typer.Option(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    outfolder: str = typer.Option(..., help="Path to the output folder"),
    identifier: str = typer.Option(..., help="Identifier of the bed file"),
    # PipelineManager
    multi: bool = typer.Option(True, help="Run multiple samples"),
    recover: bool = typer.Option(True, help="Recover from previous run"),
    dirty: bool = typer.Option(False, help="Run without removing existing files"),
):
    from bedboss.bedboss import reprocess_one as reprocess_one_function

    reprocess_one_function(
        bedbase_config=bedbase_config,
        output_folder=outfolder,
        identifier=identifier,
        pm=create_pm(outfolder=outfolder, multi=multi, recover=recover, dirty=dirty),
    )


@app.command(help="Reprocess a bedset")
def reprocess_bedset(
    bedbase_config: str = typer.Option(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    outfolder: str = typer.Option(..., help="Path to the output folder"),
    identifier: str = typer.Option(..., help="Bedset ID"),
    no_fail: bool = typer.Option(True, help="Do not fail on error"),
    heavy: bool = typer.Option(False, help="Run the heavy version of the pipeline"),
):
    from bedboss.bedboss import reprocess_bedset as reprocess_bedset_function

    reprocess_bedset_function(
        bedbase_config=bedbase_config,
        output_folder=outfolder,
        identifier=identifier,
        no_fail=no_fail,
        heavy=heavy,
    )


@app.command(help=f"Create a bed files form a [{', '.join(options_list)}] file")
def make_bed(
    input_file: str = typer.Option(
        ...,
        help="Path to the input file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    input_type: str = typer.Option(
        ...,
        help=f"Type of the input file. Options are: {', '.join(options_list)}",
        callback=validate_input_options,
        case_sensitive=False,
    ),
    outfolder: str = typer.Option(..., help="Path to the output folder"),
    genome: str = typer.Option(..., help="Genome name. Example: 'hg38'"),
    rfg_config: str = typer.Option(None, help="Path to the rfg config file"),
    narrowpeak: bool = typer.Option(False, help="Is the input file a narrowpeak file?"),
    chrom_sizes: str = typer.Option(None, help="Path to the chrom sizes file"),
    # PipelineManager
    multi: bool = typer.Option(False, help="Run multiple samples"),
    recover: bool = typer.Option(True, help="Recover from previous run"),
    dirty: bool = typer.Option(False, help="Run without removing existing files"),
):
    """

    Run the bedboss pipeline for a single bed file
    """
    from bedboss.bedmaker.bedmaker import make_bed as mk_bed_func

    mk_bed_func(
        input_file=input_file,
        input_type=input_type,
        output_path=outfolder,
        genome=genome,
        narrowpeak=narrowpeak,
        rfg_config=rfg_config,
        chrom_sizes=chrom_sizes,
        pm=create_pm(outfolder=outfolder, multi=multi, recover=recover, dirty=dirty),
    )


@app.command(help="Create a bigbed files form a bed file")
def make_bigbed(
    bed_file: str = typer.Option(
        ...,
        help="Path to the input file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    outfolder: str = typer.Option(..., help="Path to the output folder"),
    genome: str = typer.Option(..., help="Genome name. Example: 'hg38'"),
    rfg_config: str = typer.Option(None, help="Path to the rfg config file"),
):
    """

    Run the bedboss pipeline for a single bed file
    """
    from bedboss.bedmaker.bedmaker import make_bigbed as mk_bigbed_func

    mk_bigbed_func(
        bed=bed_file,
        output_path=outfolder,
        genome=genome,
        rfg_config=rfg_config,
    )


@app.command(help="Create the statistics for a single bed file.")
def run_stats(
    bed_file: str = typer.Option(
        ...,
        help="Path to the bed file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    genome: str = typer.Option(..., help="Genome name. Example: 'hg38'"),
    outfolder: str = typer.Option(..., help="Path to the output folder"),
    ensdb: str = typer.Option(None, help="Path to the EnsDb database file"),
    open_signal_matrix: str = typer.Option(
        None, help="Path to the open signal matrix file"
    ),
    just_db_commit: bool = typer.Option(False, help="Just commit to the database?"),
    # PipelineManager
    multi: bool = typer.Option(False, help="Run multiple samples"),
    recover: bool = typer.Option(True, help="Recover from previous run"),
    dirty: bool = typer.Option(False, help="Run without removing existing files"),
):
    from bedboss.bedstat.bedstat import bedstat

    bedstat(
        bedfile=bed_file,
        genome=genome,
        outfolder=outfolder,
        ensdb=ensdb,
        open_signal_matrix=open_signal_matrix,
        just_db_commit=just_db_commit,
        pm=create_pm(outfolder=outfolder, multi=multi, recover=recover, dirty=dirty),
    )


@app.command(
    help="Reindex the bedbase database and insert all files to the qdrant database."
)
def reindex(
    bedbase_config: str = typer.Option(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    purge: bool = typer.Option(False, help="Purge existing index before reindexing"),
    batch: int = typer.Option(1000, help="Number of items to upload in one batch"),
):
    from bedboss.qdrant_index.qdrant_index import add_to_qdrant

    add_to_qdrant(config=bedbase_config, batch=batch, purge=purge)


@app.command(help="Reindex semantic (text) search.")
def reindex_text(
    bedbase_config: str = typer.Option(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    purge: bool = typer.Option(False, help="Purge existing index before reindexing"),
    batch: int = typer.Option(1000, help="Number of items to upload in one batch"),
):
    from bedboss.qdrant_index.qdrant_index import reindex_semantic_search

    return reindex_semantic_search(
        config=bedbase_config,
        purge=purge,
        batch=batch,
    )


@app.command(
    help="Create a bedset from a pep file, and insert it to the bedbase database."
)
def make_bedset(
    pep: str = typer.Option(..., help="PEP file. Local or remote path"),
    outfolder: str = typer.Option(..., help="Path to the output folder"),
    bedbase_config: str = typer.Option(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    bedset_name: str = typer.Option(..., help="Name of the bedset"),
    heavy: bool = typer.Option(False, help="Run the heavy version of the pipeline"),
    force_overwrite: bool = typer.Option(
        False, help="Force overwrite the output files"
    ),
    upload_s3: bool = typer.Option(False, help="Upload to S3"),
    upload_pephub: bool = typer.Option(False, help="Upload to PEPHub"),
    no_fail: bool = typer.Option(False, help="Do not fail on error"),
):
    from bedboss.bedbuncher.bedbuncher import run_bedbuncher_form_pep

    run_bedbuncher_form_pep(
        bedbase_config=bedbase_config,
        bedset_pep=pep,
        output_folder=outfolder,
        bedset_name=bedset_name,
        heavy=heavy,
        upload_pephub=upload_pephub,
        upload_s3=upload_s3,
        no_fail=no_fail,
        force_overwrite=force_overwrite,
    )


@app.command(help="Initialize the new, sample configuration file")
def init_config(
    outfolder: str = typer.Option(..., help="Path to the output folder"),
):
    from bedboss.utils import save_example_bedbase_config

    save_example_bedbase_config(outfolder)


@app.command(help="Delete bed from the bedbase database")
def delete_bed(
    sample_id: str = typer.Option(..., help="Sample ID"),
    config: str = typer.Option(..., help="Path to the bedbase config file"),
):
    from bbconf import BedBaseAgent

    bbagent = BedBaseAgent(config)
    bbagent.bed.delete(sample_id)
    print(f"sample {sample_id} deleted from the bedbase database")


def version_callback(value: bool):
    if value:
        typer.echo(f"Bedboss version: {__version__}")
        raise typer.Exit()


@app.command(help="Delete BedSet from the bedbase database")
def delete_bedset(
    identifier: str = typer.Option(..., help="BedSet ID"),
    config: str = typer.Option(..., help="Path to the bedbase config file"),
):
    from bbconf import BedBaseAgent

    bbagent = BedBaseAgent(config)
    bbagent.bedset.delete(identifier)
    print(f"BedSet {identifier} deleted from the bedbase database")


@app.command(help="Tokenize a bedfile")
def tokenize_bed(
    bed_id: str = typer.Option(
        ...,
        help="Path to the bed file",
    ),
    universe_id: str = typer.Option(
        ...,
        help="Universe ID",
    ),
    cache_folder: str = typer.Option(
        None,
        help="Path to the cache folder",
    ),
    add_to_db: bool = typer.Option(
        False,
        help="Add the tokenized bed file to the bedbase database",
    ),
    bedbase_config: str = typer.Option(
        None,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    overwrite: bool = typer.Option(
        False,
        help="Overwrite the existing tokenized bed file",
    ),
):
    from bedboss.tokens.tokens import tokenize_bed_file

    tokenize_bed_file(
        universe=universe_id,
        bed=bed_id,
        cache_folder=cache_folder,
        add_to_db=add_to_db,
        config=bedbase_config,
        overwrite=overwrite,
    )


@app.command(help="Delete tokenized bed file")
def delete_tokenized(
    universe_id: str = typer.Option(
        ...,
        help="Universe ID",
    ),
    bed_id: str = typer.Option(
        ...,
        help="Bed ID",
    ),
    config: str = typer.Option(
        None,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
):
    from bedboss.tokens.tokens import delete_tokenized

    delete_tokenized(
        universe=universe_id,
        bed=bed_id,
        config=config,
    )


@app.command(help="Convert bed file to universe")
def convert_universe(
    bed_id: str = typer.Option(
        ...,
        help="Path to the bed file",
    ),
    config: str = typer.Option(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    method: str = typer.Option(
        None,
        help="Method used to create the universe",
    ),
    bedset: str = typer.Option(
        None,
        help="Bedset used to create the universe",
    ),
):
    from bbconf.bbagent import BedBaseAgent

    bbagent = BedBaseAgent(config)
    bbagent.bed.add_universe(
        bedfile_id=bed_id,
        bedset_id=bedset,
        construct_method=method,
    )


@app.command(help="Update reference genomes in the database")
def update_genomes(
    config: str = typer.Option(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
):
    from bbconf.bbagent import BedBaseAgent
    from bedboss.refgenome_validator.refgenie_chrom_sizes import update_db_genomes

    bbagent = BedBaseAgent(config)
    update_db_genomes(bbagent)

    print("Genomes updated successfully.")


@app.command(help="Download UMAP")
def download_umap(
    config: str = typer.Option(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    output_file: str = typer.Option(
        ...,
        help="Path to the output json file where UMAP embeddings will be saved. (*Python version will be added to the filename)",
    ),
    n_components: int = typer.Option(
        2,
        help="Number of UMAP components",
    ),
    plot_name: str = typer.Option(
        None,
        help="Name of the plot file",
    ),
    plot_label: str = typer.Option(
        None,
        help="Label for the plot",
    ),
    top_assays: int = typer.Option(
        15,
        help="Number of top assays to include",
    ),
    top_cell_lines: int = typer.Option(
        15,
        help="Number of top cell lines to include",
    ),
    method: str = typer.Option(
        "umap",
        help="Dimensionality reduction method to use. Options: 'umap', 'pca', or 'tsne'. To use UMAP, 'umap-learn' package must be installed.",
    ),
):
    from bedboss.scripts.make_umap import get_embeddings

    get_embeddings(
        bbconf=config,
        output_file=output_file,
        n_components=n_components,
        plot_name=plot_name,
        plot_label=plot_label,
        top_assays=top_assays,
        top_cell_lines=top_cell_lines,
        method=method,
    )


@app.command(help="Check installed R packages")
def check_requirements():
    from bedboss.bedboss import requirements_check

    print("Checking pipelines requirements...")
    requirements_check()


@app.command(help="Install R dependencies")
def install_requirements():
    import subprocess

    r_path = os.path.abspath(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "scripts", "installRdeps.R"
        )
    )

    subprocess.run(
        ["Rscript", r_path],
    )


@app.command(help="Verify configuration file")
def verify_config(
    config: str = typer.Argument(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
):
    from bbconf.config_parser.utils import config_analyzer

    try:
        config_analyzer(config)
    except Exception as e:
        printm.print_error(f"Error in provided configuration file: {e}")
        raise typer.Exit(code=1)
    typer.Exit(code=0)


@app.command(help="Get available commands", hidden=True)
def get_commands():
    print(
        " ".join([k.callback.__name__ for k in app.registered_commands]).replace(
            "_", "-"
        )
    )


@app.callback()
def common(
    ctx: typer.Context,
    version: bool = typer.Option(
        None, "--version", "-v", callback=version_callback, help="App version"
    ),
):
    pass


app.add_typer(app_bbuploader, name="geo")
