from peppy import Project
import yaml
import logmuse
import os
from typing import NoReturn
from argparse import ArgumentParser
import sys

_LOGGER = logmuse.init_logger(name="yaml_creator")


def save_yaml_form_dict(info_dict: dict, full_path: str, name: str) -> NoReturn:
    """
    Save yaml files with metadata that will be later used in bedbase
    :param info_dict: dict with metadata that have to be writen do yaml
    :param full_path: path to folder where yaml files have to be saved
    :return: NoReturn
    """
    if not os.path.isdir(full_path):
        os.makedirs(full_path)
    path_name = os.path.join(full_path, f"{name}.yaml")
    with open(path_name, "w+") as f:
        yaml.dump(info_dict, f)


def create_sample_yaml(
    sample_csv_path: str, output_path: str, columns: list = None
) -> NoReturn:
    """
    Creating sample yaml out of sample csv
    :param sample_csv_path:
    :param output_path:
    :param columns:
    :return: NoReturn
    """
    proj = Project(sample_csv_path)
    for s in proj.samples:
        s_dict = s.to_dict()
        if columns:
            sample_dict = {}
            for col in columns:
                try:
                    sample_dict[col] = s_dict[col]
                except KeyError:
                    _LOGGER.warning(f"No '{col}' column was found!")
        else:
            sample_dict = s_dict
        save_yaml_form_dict(sample_dict, output_path, s["sample_name"])


# example
# create_sample_yaml("/home/bnt4me/Virginia/repos/bedbase/docs_jupyter/bedbase_tutorial/bedbase/tutorial_files/bedboss/bedstat_annotation_sheet.csv",
#                    "../test_f2/meta", columns=["GSE","GSM"])
def _parse_cmdl():
    parser = ArgumentParser(
        description="Create yaml's with metadata",
    )
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="sample file (csv)",
        type=str,
    )
    parser.add_argument(
        "-f",
        "--folder",
        required=True,
        help="sample file (csv)",
    )
    parser.add_argument(
        "-c",
        "--columns",
        required=False,
        help="Columns, that you want to save. Enter column type: 'col1, col2, col3'",
    )

    args = parser.parse_args(sys.argv[1:])
    return args


def main():
    args = _parse_cmdl()
    args_dict = vars(args)
    create_sample_yaml(
        sample_csv_path=args_dict["input"], output_path=args_dict["folder"]
    )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
