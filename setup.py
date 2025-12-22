#! /usr/bin/env python

import os
import sys

from setuptools import setup

PACKAGE_NAME = "bedboss"
REQDIR = "requirements"

# Additional keyword arguments for setup().
extra = {}

with open(f"{PACKAGE_NAME}/_version.py", "r") as versionfile:
    __version__ = versionfile.readline().split()[-1].strip("\"'\n")


def read_reqs(reqs_name):
    deps = []
    with open(os.path.join(REQDIR, f"requirements-{reqs_name}.txt"), "r") as file:
        for line in file:
            if not line.strip():
                continue
            deps.append(line)
    return deps


DEPENDENCIES = read_reqs("all")
extra["install_requires"] = DEPENDENCIES

scripts = None

with open("README.md") as f:
    long_description = f.read()

setup(
    name=PACKAGE_NAME,
    packages=[PACKAGE_NAME],
    version=__version__,
    description="Pipelines for genomic region file to produce bed files, "
    "and it's statistics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
    ],
    keywords="project, bioinformatics, sequencing, ngs, workflow",
    url="https://databio.org",
    authors=[
        "Oleksandr Khoroshevskyi",
        "Michal Stolarczyk",
        "Ognen Duzlevski",
        "Jose Verdezoto",
        "Bingjie Xue",
    ],
    author_email="khorosh@virginia.edu",
    license="BSD2",
    entry_points={
        "console_scripts": [
            "bedboss = bedboss.__main__:main",
        ],
    },
    package_data={PACKAGE_NAME: ["templates/*"]},
    include_package_data=True,
    test_suite="tests",
    tests_require=read_reqs("dev"),
    setup_requires=(
        ["pytest-runner"] if {"test", "pytest", "ptr"} & set(sys.argv) else []
    ),
    **extra,
)
