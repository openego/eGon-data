"""The central module containing or importing essential code dealing with
creating medium-voltage grid district related dataset .

"""


from egon.data.datasets import Dataset

import egon.data.processing.mv_grid_districts as mvgd
import egon.data.processing.substation as substation


class MVGridDistricts(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="MV_grid_districts",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(substation.create_voronoi,
                   mvgd.define_mv_grid_districts,
                   ),
        )
