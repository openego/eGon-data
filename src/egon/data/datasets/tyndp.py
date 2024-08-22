"""The central module containing all code dealing with tyndp data
"""

import os
from egon.data import config
from egon.data.datasets import Dataset
from urllib.request import urlretrieve

class Tyndp(Dataset):

    def __init__(self, dependencies):
        super().__init__(
            name="Tyndp",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(download),
        )

def download():
    """ Download input data from TYNDP 2020
    Returns
    -------
    None.
    """
    sources = config.datasets()["tyndp"]["sources"]
    targets = config.datasets()["tyndp"]["targets"]

    if not os.path.exists('tyndp'):
        os.mkdir('tyndp')

    for dataset in ['capacities', 'demand_2030', 'demand_2040']:

        target_file = targets[dataset]

        urlretrieve(sources[dataset], f"tyndp/{target_file}")
