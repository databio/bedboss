

# Define pipeline chunks (to be in separate files)

def bedmaker(pm: pypiper.PipelineManager, sample: peppy.Sample, params: dict):
	...
	pm.run("command 1")

def bedqc(pm: pypiper.PipelineManager, sample: peppy.Sample, params: dict):
	...

	pm.run("some qc")

    # check number of regions
    script_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "bedmaker",
        "../bedmaker/est_line.sh",
    )
    cmd = f"bash {script_path} {file} "
    approx_n_lines = pm.checkprint(cmd, lock_name=next(tempfile._get_candidate_names()))
    pm.report_result("approx_n_lines", approx_n_lines)

    ...

    return True

def bedstat(pm: pypiper.PipelineManager, sample: peppy.Sample, params: dict):
	...
	pm.run("some qc")

from . import bedmaker
from . import bedqc
from . import bedstat


# bedboss run all --sample_name sample1 --bedfile mybed.bed --min_width 3
# bedboss run bedstat --sample_name sample1 --bedfile mybed.bed --min_width 3

def __main__():
	args = parse_args()

	# A sample is a BED file; it's one row in the table.
	sample = read_sample(args)

	# Maybe these are pipeline-level attributes that aren't part of the sample.
	params = process_params(args)

	chunk_map = {
		"all": [bedmaker, bedqc, bedstat],
		"stat": [bedstat],
		"qc": [bedqc],
		"make": [bedmaker]
	}

	pm = new pypiper.PipelineManager()
	pm.start_pipeline()

	for chunk_function in chunk_map[params.pipeline]:
		try:
			chunk_function(pm, sample, params)
		except Exception as e:
			_LOGGER.error(e)
			raise e

	pm.stop_pipeline()
