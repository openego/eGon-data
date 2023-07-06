#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from __future__ import absolute_import, print_function

from glob import glob
from os.path import basename, dirname, join, splitext
import io
import re

from setuptools import find_packages, setup


def read(*names, **kwargs):
    with io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8"),
    ) as fh:
        return fh.read()


setup(
    name="egon.data",
    version="0.0.0",
    license="AGPL-3.0-or-later",
    description=(
        "The data used in the eGo^N project along with the code importing, "
        "generating and processing it."
    ),
    long_description="%s\n%s"
    % (
        re.compile("^.. start-badges.*^.. end-badges", re.M | re.S).sub(
            "", read("README.rst")
        ),
        re.sub(":[a-z]+:`~?(.*?)`", r"``\1``", read("CHANGELOG.rst")),
    ),
    author="Guido Pleßmann, Ilka Cußman, Stephan Günther",
    author_email=(
        "guido.plessmann@rl-institut.de, "
        "ilka.cussmann@hs-flensburg.de, "
        "stephan.guenther@ovgu.de"
    ),
    url="https://github.com/openego/eGon-data",
    packages=["egon"] + ["egon." + p for p in find_packages("src/egon")],
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list:
        # http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        (
            "License :: OSI Approved :: "
            "GNU Affero General Public License v3 or later (AGPLv3+)"
        ),
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
        # uncomment if you test on these interpreters:
        # "Programming Language :: Python :: Implementation :: PyPy",
        # "Programming Language :: Python :: Implementation :: IronPython",
        # "Programming Language :: Python :: Implementation :: Jython",
        # "Programming Language :: Python :: Implementation :: Stackless",
        "Topic :: Utilities",
    ],
    project_urls={
        "Documentation": "https://eGon-data.readthedocs.io/",
        "Changelog": (
            "https://eGon-data.readthedocs.io/en/latest/changelog.html"
        ),
        "Issue Tracker": "https://github.com/openego/eGon-data/issues",
    },
    keywords=[
        # eg: 'keyword1', 'keyword2', 'keyword3',
    ],
    python_requires=">=3.7",
    install_requires=[
        # eg: 'aspectlib==1.1.1', 'six>=1.7',
        "apache-airflow>2.0",
        "apache-airflow-providers-sendgrid",
        "atlite==0.2.5",
        "cdsapi",
        "click",
        #"entsoe-py >=0.3.1",
        "geopandas>=0.10.0",
        "geopy",
        "geovoronoi",
        "importlib-resources",
        "loguru",
        "markupsafe", 
        "matplotlib",
        "netcdf4",
        "numpy",
        "omi",
        "openpyxl",
        "pandas>1.2.0",
        "psycopg2",
        "pyaml",
        "pypsa==0.17.1",
        "rasterio",
        "requests",
        "rioxarray",
        "rtree",
        "saio",
        "seaborn",
        "shapely",
        "snakemake<7",
        "sqlalchemy",
        "wtforms",
        "xarray",
        "xlrd",
    ],
    extras_require={
        "dev": ["black", "flake8", "isort>=5", "pre-commit", "pytest", "tox"]
        # eg:
        #   'rst': ['docutils>=0.11'],
        #   ':python_version=="2.6"': ['argparse'],
    },
    entry_points={"console_scripts": ["egon-data = egon.data.cli:main"]},
)
