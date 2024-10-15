from egon.data.datasets import Dataset
from egon.data.datasets.scenario_path.import_status2019 import (
    import_scn_status2019,
)


class CreateIntermediateScenarios(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="scenario_path",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(import_scn_status2019,),
        )
