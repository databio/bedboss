import typer
from typing import Union
import os
import pypiper

from bedboss.bedqc.bedqc import bedqc
from bedboss.const import MAX_FILE_SIZE, MAX_REGION_NUMBER, MIN_REGION_WIDTH

from bedboss import __version__

app = typer.Typer(pretty_exceptions_short=False, pretty_exceptions_show_locals=False)


def create_pm(
    outfolder: str, multi: bool = False, recover: bool = True, dirty: bool = False
) -> pypiper.PipelineManager:
    pm_out_folder = outfolder
    pm_out_folder = os.path.join(pm_out_folder, "pipeline_manager")

    pm = pypiper.PipelineManager(
        name="bedboss-pipeline",
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
    upload_qdrant: bool = typer.Option(False, help="Upload to Qdrant"),
    upload_s3: bool = typer.Option(False, help="Upload to S3"),
    upload_pephub: bool = typer.Option(False, help="Upload to PEPHub"),
    # PipelineManager
    multi: bool = typer.Option(False, help="Run multiple samples"),
    recover: bool = typer.Option(True, help="Recover from previous run"),
    dirty: bool = typer.Option(False, help="Run without removing existing files"),
):
    """

    Run the bedboss pipeline for a single bed file
    """
    from bedboss.bedboss import run_all as run_all_bedboss

    run_all_bedboss(
        input_file=input_file,
        input_type=input_type,
        outfolder=outfolder,
        genome=genome,
        bedbase_config=bedbase_config,
        rfg_config=rfg_config,
        narrowpeak=narrowpeak,
        check_qc=check_qc,
        chrom_sizes=chrom_sizes,
        open_signal_matrix=open_signal_matrix,
        ensdb=ensdb,
        other_metadata=None,
        just_db_commit=just_db_commit,
        force_overwrite=force_overwrite,
        upload_qdrant=upload_qdrant,
        upload_s3=upload_s3,
        upload_pephub=upload_pephub,
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
    bedset_id: Union[str, None] = typer.Option(None, help="Bedset ID"),
    rfg_config: str = typer.Option(None, help="Path to the rfg config file"),
    check_qc: bool = typer.Option(True, help="Check the quality of the input file?"),
    ensdb: str = typer.Option(None, help="Path to the EnsDb database file"),
    just_db_commit: bool = typer.Option(False, help="Just commit to the database?"),
    force_overwrite: bool = typer.Option(
        False, help="Force overwrite the output files"
    ),
    upload_qdrant: bool = typer.Option(False, help="Upload to Qdrant"),
    upload_s3: bool = typer.Option(False, help="Upload to S3"),
    upload_pephub: bool = typer.Option(False, help="Upload to PEPHub"),
    no_fail: bool = typer.Option(False, help="Do not fail on error"),
    # PipelineManager
    multi: bool = typer.Option(False, help="Run multiple samples"),
    recover: bool = typer.Option(True, help="Recover from previous run"),
    dirty: bool = typer.Option(False, help="Run without removing existing files"),
):
    """
    Run the bedboss pipeline for a bed files in a PEP
    """
    from bedboss.bedboss import insert_pep

    insert_pep(
        bedbase_config=bedbase_config,
        output_folder=outfolder,
        pep=pep,
        bedset_id=bedset_id,
        rfg_config=rfg_config,
        create_bedset=create_bedset,
        check_qc=check_qc,
        ensdb=ensdb,
        just_db_commit=just_db_commit,
        force_overwrite=force_overwrite,
        upload_s3=upload_s3,
        upload_pephub=upload_pephub,
        upload_qdrant=upload_qdrant,
        no_fail=no_fail,
        pm=create_pm(
            outfolder=outfolder,
            multi=multi,
            recover=recover,
            dirty=dirty,
        ),
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


@app.command(help=f"Create a bigbed files form a bed file")
def make_bigbed(
    bed_file: str = typer.Option(
        ...,
        help="Path to the input file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    bed_type: str = typer.Option(
        ...,
        help="bed type to be used for bigBed file generation 'bed{bedtype}+{n}' [Default: None] (e.g bed3+1)",
    ),
    outfolder: str = typer.Option(..., help="Path to the output folder"),
    genome: str = typer.Option(..., help="Genome name. Example: 'hg38'"),
    rfg_config: str = typer.Option(None, help="Path to the rfg config file"),
    chrom_sizes: str = typer.Option(None, help="Path to the chrom sizes file"),
    # PipelineManager
    multi: bool = typer.Option(False, help="Run multiple samples"),
    recover: bool = typer.Option(True, help="Recover from previous run"),
    dirty: bool = typer.Option(False, help="Run without removing existing files"),
):
    """

    Run the bedboss pipeline for a single bed file
    """
    from bedboss.bedmaker.bedmaker import make_bigbed as mk_bigbed_func

    mk_bigbed_func(
        bed_path=bed_file,
        output_path=outfolder,
        genome=genome,
        bed_type=bed_type,
        rfg_config=rfg_config,
        chrom_sizes=chrom_sizes,
        pm=create_pm(outfolder=outfolder, multi=multi, recover=recover, dirty=dirty),
    )


@app.command(help="Run the quality control for a bed file")
def run_qc(
    bed_file: str = typer.Option(
        ...,
        help="Path to the bed file to check the quality control on.",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    outfolder: str = typer.Option(..., help="Path to the output folder"),
    max_file_size: int = typer.Option(
        MAX_FILE_SIZE, help="Maximum file size threshold to pass the quality"
    ),
    max_region_number: int = typer.Option(
        MAX_REGION_NUMBER,
        help="Maximum number of regions threshold to pass the quality",
    ),
    min_region_width: int = typer.Option(
        MIN_REGION_WIDTH, help="Minimum region width threshold to pass the quality"
    ),
    # PipelineManager
    multi: bool = typer.Option(False, help="Run multiple samples"),
    recover: bool = typer.Option(True, help="Recover from previous run"),
    dirty: bool = typer.Option(False, help="Run without removing existing files"),
):
    bedqc(
        bedfile=bed_file,
        outfolder=outfolder,
        max_file_size=max_file_size,
        max_region_number=max_region_number,
        min_region_width=min_region_width,
        pm=create_pm(outfolder=outfolder, multi=multi, recover=recover, dirty=dirty),
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
):
    from bedboss.qdrant_index.qdrant_index import add_to_qdrant

    add_to_qdrant(config=bedbase_config)


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


@app.callback()
def common(
    ctx: typer.Context,
    version: bool = typer.Option(
        None, "--version", "-v", callback=version_callback, help="App version"
    ),
):
    pass
