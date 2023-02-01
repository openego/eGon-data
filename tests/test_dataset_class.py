from dataclasses import dataclass
from typing import Union

from airflow.models.dag import DAG

from egon.data.datasets import Dataset, TaskGraph, Tasks


def test_uniqueness_of_automatically_generated_final_dataset_task():
    """Test that the generated final dataset task is named uniquely.

    This is a regression test for issue #985. Having multiple `Dataset`s ending
    in parallel tasks doesn't work if those `Dataset`s are in a module below
    the `egon.data.datasets` package. In that case the code removing the module
    name prefix from task ids and the code generating the final dataset task
    which updates the dataset version once all parallel tasks have finished
    interact in a way that generates non-distinct task ids so that tasks
    generated later clobber the ones generated earlier. This leads to spurious
    cycles and other inconsistencies and bugs in the graph.
    """

    noops = [(lambda: None) for _ in range(4)]
    for i, noop in enumerate(noops):
        noop.__name__ = f"noop-{i}"

    @dataclass
    class Dataset_1(Dataset):
        name: str = "DS1"
        version: str = "0.0.0"
        tasks: Union[Tasks, TaskGraph] = ({noops[0], noops[1]},)

    @dataclass
    class Dataset_2(Dataset):
        name: str = "DS2"
        version: str = "0.0.0"
        tasks: Union[Tasks, TaskGraph] = ({noops[2], noops[3]},)

    Dataset_1.__module__ = "egon.data.datasets.test.datasets"
    Dataset_2.__module__ = "egon.data.datasets.test.datasets"
    with DAG(dag_id="Test-DAG", default_args={"start_date": "1111-11-11"}):
        datasets = [Dataset_1(), Dataset_2()]
    ids = [list(dataset.tasks)[-1] for dataset in datasets]
    assert (
        ids[0] != ids[1]
    ), "Expected unique names for final tasks of distinct datasets."
