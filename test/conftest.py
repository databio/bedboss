import os


def get_bed_path(file_correctness, file_name):
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "tests",
        "data",
        "bed",
        "hg19",
        file_correctness,
        file_name,
    )


def get_list_of_correct_bed():
    get_bed_path
