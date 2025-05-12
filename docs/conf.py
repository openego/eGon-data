# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    "sphinx.ext.extlinks",
    "sphinx.ext.ifconfig",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
]
source_suffix = ".rst"
master_doc = "index"
project = "eGo^N Data"
year = "2020-2022"
author = "Guido Pleßmann, Ilka Cußman, Stephan Günther"
copyright = "{0}, {1}".format(year, author)
version = release = "1.0.0"

pygments_style = "trac"
templates_path = ["."]
extlinks = {
    "issue": ("https://github.com/openego/eGon-data/issues/%s", "issue #"),
    "pr": ("https://github.com/openego/eGon-data/pull/%s", "PR #"),
}
# on_rtd is whether we are on readthedocs.org
# on_rtd = os.environ.get("READTHEDOCS", None) == "True"

#if not on_rtd:  # only set the theme if we're building docs locally
html_theme = "sphinx_rtd_theme"

html_use_smartypants = True
html_last_updated_fmt = "%b %d, %Y"
html_split_index = False
html_sidebars = {"**": ["searchbox.html", "globaltoc.html", "sourcelink.html"]}
html_short_title = "%s-%s" % (project, version)

napoleon_use_ivar = True
napoleon_use_rtype = False
napoleon_use_param = False

add_module_names = False
modindex_common_prefix = ["egon.data.", "egon.data.datasets."]

autodoc_type_aliases = {
    "Dependencies": "egon.data.datasets.Dependencies",
    "Tasks": "egon.data.datasets.Tasks"
}
