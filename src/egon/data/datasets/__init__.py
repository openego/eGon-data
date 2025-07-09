"""The API for configuring datasets."""

from __future__ import annotations

from collections import abc
from dataclasses import dataclass, field
from functools import partial, reduce, update_wrapper
from typing import Callable, Dict, Iterable, Set, Tuple, Union
import re

import json
from pathlib import Path
from airflow.models.baseoperator import BaseOperator as Operator
from airflow.operators.python import PythonOperator
from sqlalchemy import Column, ForeignKey, Integer, String, Table, orm, tuple_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB

from egon.data import config, db, logger

Base = declarative_base()
SCHEMA = "metadata"


def wrapped_partial(func, *args, **kwargs):
    """Like :func:`functools.partial`, but preserves the original function's
    name and docstring. Also allows to add a postfix to the function's name.
    """
    postfix = kwargs.pop("postfix", None)
    partial_func = partial(func, *args, **kwargs)
    update_wrapper(partial_func, func)
    if postfix:
        partial_func.__name__ = f"{func.__name__}{postfix}"
    return partial_func


def setup():
    """Create the database structure for storing dataset information."""
    # TODO: Move this into a task generating the initial database structure.
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
    Model.__table__.create(bind=db.engine(), checkfirst=True)
    DependencyGraph.create(bind=db.engine(), checkfirst=True)


# TODO: Figure out how to use a mapped class as an association table.
#
# Trying it out, I ran into quite a few problems and didn't have time to do
# further research. The benefits are mostly just convenience, so it doesn't
# have a high priority. But I'd like to keep the code I started out with to
# have a starting point for me or anybody else trying again.
#
# class DependencyGraph(Base):
#     __tablename__ = "dependency_graph"
#     __table_args__ = {"schema": SCHEMA}
#     dependency_id = Column(Integer, ForeignKey(Model.id), primary_key=True,)
#     dependent_id = Column(Integer, ForeignKey(Model.id), primary_key=True,)

DependencyGraph = Table(
    "dependency_graph",
    Base.metadata,
    Column(
        "dependency_id",
        Integer,
        ForeignKey(f"{SCHEMA}.datasets.id"),
        primary_key=True,
    ),
    Column(
        "dependent_id",
        Integer,
        ForeignKey(f"{SCHEMA}.datasets.id"),
        primary_key=True,
    ),
    schema=SCHEMA,
)


class Model(Base):
    __tablename__ = "datasets"
    __table_args__ = {"schema": SCHEMA}
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    version = Column(String, nullable=False)
    epoch = Column(Integer, default=0)
    scenarios = Column(String, nullable=False)
    sources = Column(JSONB, nullable=True)
    targets = Column(JSONB, nullable=True)

    dependencies = orm.relationship(
        "Model",
        secondary=DependencyGraph,
        primaryjoin=id == DependencyGraph.c.dependent_id,
        secondaryjoin=id == DependencyGraph.c.dependency_id,
        backref=orm.backref("dependents", cascade="all, delete"),
    )


@dataclass
class DatasetSources:
    tables: Dict[str, str] = field(default_factory=dict)
    files: Dict[str, str] = field(default_factory=dict)
    urls: Dict[str, str] = field(default_factory=dict)

    def empty(self):
        return not (self.tables or self.files or self.urls)

    def get_table_schema(self, key: str) -> str:
        """Returns the schema of the table identified by key."""
        try:
            return self.tables[key].split(".", 1)[0]
        except (KeyError, AttributeError, IndexError):
            raise ValueError(f"Invalid table reference: {self.tables.get(key)}")

    def get_table_name(self, key: str) -> str:
        """Returns the table name of the table identified by key."""
        try:
            return self.tables[key].split(".", 1)[1]
        except (KeyError, AttributeError, IndexError):
            raise ValueError(f"Invalid table reference: {self.tables.get(key)}")

    def to_dict(self):
        return {
            "tables": self.tables,
            "urls": self.urls,
            "files": self.files,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            tables=data.get("tables", {}),
            urls=data.get("urls", {}),
            files=data.get("files", {}),
        )

@dataclass
class DatasetTargets:
    tables: Dict[str, str] = field(default_factory=dict)
    files: Dict[str, str] = field(default_factory=dict)

    def empty(self):
        return not (self.tables or self.files)

    def get_table_schema(self, key: str) -> str:
        """Returns the schema of the table identified by key."""
        try:
            return self.tables[key].split(".", 1)[0]
        except (KeyError, AttributeError, IndexError):
            raise ValueError(f"Invalid table reference: {self.tables.get(key)}")

    def get_table_name(self, key: str) -> str:
        """Returns the table name of the table identified by key."""
        try:
            return self.tables[key].split(".", 1)[1]
        except (KeyError, AttributeError, IndexError):
            raise ValueError(f"Invalid table reference: {self.tables.get(key)}")

    def to_dict(self):
        return {
            "tables": self.tables,
            "files": self.files,
        }

    def from_dict(cls, data):
        return cls(
            tables=data.get("tables", {}),
            files=data.get("files", {}),
        )

#: A :class:`Task` is an Airflow :class:`Operator` or any
#: :class:`Callable <typing.Callable>` taking no arguments and returning
#: :obj:`None`. :class:`Callables <typing.Callable>` will be converted
#: to :class:`Operators <Operator>` by wrapping them in a
#: :class:`PythonOperator` and setting the :obj:`~PythonOperator.task_id`
#: to the :class:`Callable <typing.Callable>`'s
#: :obj:`~definition.__name__`, with underscores replaced with hyphens.
#: If the :class:`Callable <typing.Callable>`'s `__module__`__ attribute
#: contains the string :obj:`"egon.data.datasets."`, the
#: :obj:`~PythonOperator.task_id` is also prefixed with the module name,
#: followed by a dot and with :obj:`"egon.data.datasets."` removed.
#:
#: __ https://docs.python.org/3/reference/datamodel.html#index-34
Task = Union[Callable[[], None], Operator]
#: A graph of tasks is, in its simplest form, just a single node, i.e. a
#: single :class:`Task`. More complex graphs can be specified by nesting
#: :class:`sets <builtins.set>` and :class:`tuples <builtins.tuple>` of
#: :class:`TaskGraphs <TaskGraph>`. A set of :class:`TaskGraphs
#: <TaskGraph>` means that they are unordered and can be
#: executed in parallel. A :class:`tuple` specifies an implicit ordering so
#: a :class:`tuple` of :class:`TaskGraphs <TaskGraph>` will be executed
#: sequentially in the given order.
TaskGraph = Union[Task, Set["TaskGraph"], Tuple["TaskGraph", ...]]
#: A type alias to help specifying that something can be an explicit
#: :class:`Tasks_` object or a :class:`TaskGraph`, i.e. something that
#: can be converted to :class:`Tasks_`.
Tasks = Union["Tasks_", TaskGraph]


def prefix(o):
    module = o.__module__
    parent = f"{__name__}."
    return f"{module.replace(parent, '')}." if parent in module else ""


@dataclass
class Tasks_(dict):
    first: Set[Task]
    last: Set[Task]
    graph: TaskGraph = ()

    def __init__(self, graph: TaskGraph):
        """Connect multiple tasks into a potentially complex graph.

        Parses a :class:`TaskGraph` into a :class:`Tasks_` object.
        """
        if isinstance(graph, Callable):
            graph = PythonOperator(
                task_id=f"{prefix(graph)}{graph.__name__.replace('_', '-')}",
                python_callable=graph,
            )
        self.graph = graph
        if isinstance(graph, Operator):
            self.first = {graph}
            self.last = {graph}
            self[graph.task_id] = graph
        elif isinstance(graph, abc.Sized) and len(graph) == 0:
            self.first = {}
            self.last = {}
        elif isinstance(graph, abc.Set):
            results = [Tasks_(subtasks) for subtasks in graph]
            self.first = {task for result in results for task in result.first}
            self.last = {task for result in results for task in result.last}
            self.update(reduce(lambda d1, d2: dict(d1, **d2), results, {}))
            self.graph = set(tasks.graph for tasks in results)
        elif isinstance(graph, tuple):
            results = [Tasks_(subtasks) for subtasks in graph]
            for left, right in zip(results[:-1], results[1:]):
                for last in left.last:
                    for first in right.first:
                        last.set_downstream(first)
            self.first = results[0].first
            self.last = results[-1].last
            self.update(reduce(lambda d1, d2: dict(d1, **d2), results, {}))
            self.graph = tuple(tasks.graph for tasks in results)
        else:
            raise (
                TypeError(
                    "`egon.data.datasets.Tasks_` got an argument of type:\n\n"
                    f"  {type(graph)}\n\n"
                    "where only `Task`s, `Set`s and `Tuple`s are allowed."
                )
            )


#: A dataset can depend on other datasets or the tasks of other datasets.
Dependencies = Iterable[Union["Dataset", Task]]


@dataclass
class Dataset:
    #: The name of the Dataset
    name: str
    #: The :class:`Dataset`'s version. Can be anything from a simple
    #: semantic versioning string like "2.1.3", to a more complex
    #: string, like for example "2021-01-01.schleswig-holstein.0" for
    #: OpenStreetMap data.
    #: Note that the latter encodes the :class:`Dataset`'s date, region
    #: and a sequential number in case the data changes without the date
    #: or region changing, for example due to implementation changes.
    version: str
    #: The sources used by the datasets.
    #: Could be tables, files and urls
    sources: DatasetSources = field(init=False)
    #: The targets created by the datasets.
    #: Could be tables and files
    targets: DatasetTargets = field(init=False)
    #: The first task(s) of this :class:`Dataset` will be marked as
    #: downstream of any of the listed dependencies. In case of bare
    #: :class:`Task`, a direct link will be created whereas for a
    #: :class:`Dataset` the link will be made to all of its last tasks.
    dependencies: Dependencies = ()
    #: The tasks of this :class:`Dataset`. A :class:`TaskGraph` will
    #: automatically be converted to :class:`Tasks_`.
    tasks: Tasks = ()

    def check_version(self, after_execution=()):
        scenario_names = config.settings()["egon-data"]["--scenarios"]

        def skip_task(task, *xs, **ks):
            with db.session_scope() as session:
                datasets = session.query(Model).filter_by(name=self.name).all()
                if (
                    self.version in [ds.version for ds in datasets]
                    and scenario_names
                    == [
                        ds.scenarios.replace("{", "").replace("}", "")
                        for ds in datasets
                    ]
                    and not re.search(r"\.dev$", self.version)
                ):
                    logger.info(
                        f"Dataset '{self.name}' version '{self.version}'"
                        f" scenarios {scenario_names}"
                        f" already executed. Skipping."
                    )
                else:
                    for ds in datasets:
                        session.delete(ds)
                    result = super(type(task), task).execute(*xs, **ks)
                    for function in after_execution:
                        function(session)
                    return result

        return skip_task

    def update(self, session):
        dataset = Model(
            name=self.name,
            version=self.version,
            scenarios=config.settings()["egon-data"]["--scenarios"],
            sources=self.sources.to_dict() if hasattr(self.sources, "to_dict") else dict(self.sources),
            targets=self.targets.to_dict() if hasattr(self.targets, "to_dict") else dict(self.targets),
        )

        dependencies = (
            session.query(Model)
            .filter(
                tuple_(Model.name, Model.version).in_(
                    [
                        (dataset.name, dataset.version)
                        for dependency in self.dependencies
                        if isinstance(dependency, Dataset)
                        or hasattr(dependency, "dataset")
                        for dataset in [
                            dependency.dataset
                            if isinstance(dependency, Operator)
                            else dependency
                        ]
                    ]
                )
            )
            .all()
        )
        dataset.dependencies = dependencies
        session.add(dataset)

    def __post_init__(self):
        self.dependencies = list(self.dependencies)

        class_sources = getattr(type(self), "sources", None)

        if not isinstance(class_sources, DatasetSources):
            logger.warning(
                f"Dataset '{type(self).__name__}' has no valid class-level 'sources' attribute. "
                "Defaulting to empty DatasetSources().",
                stacklevel=2
            )
            self.sources = DatasetSources()
        else:
            self.sources = class_sources
            if self.sources.empty():
                logger.warning(
                    f"Dataset '{type(self).__name__}' defines 'sources', but it is empty. "
                    "Please check if this is intentional.",
                    stacklevel=2
                )


        class_targets = getattr(type(self), "targets", None)

        if not isinstance(class_targets, DatasetTargets):
            logger.warning(
                f"Dataset '{type(self).__name__}' has no valid class-level 'targets' attribute. "
                "Defaulting to empty DatasetTargets().",
                stacklevel=2
            )
            self.targets = DatasetTargets()
        else:
            self.targets = class_targets
            if self.targets.empty():
                logger.warning(
                    f"Dataset '{type(self).__name__}' defines 'targets', but it is empty. "
                    "Please check if this is intentional.",
                    stacklevel=2
                )
        if not isinstance(self.tasks, Tasks_):
            self.tasks = Tasks_(self.tasks)
        if len(self.tasks.last) > 1:
            # Explicitly create single final task, because we can't know
            # which of the multiple tasks finishes last.
            name = prefix(self)
            name = f"{name if name else f'{self.__module__}.'}{self.name}."
            update_version = PythonOperator(
                task_id=f"{name}update-version",
                # Do nothing, because updating will be added later.
                python_callable=lambda *xs, **ks: None,
            )
            self.tasks = Tasks_((self.tasks.graph, update_version))
        # Due to the `if`-block above, there'll now always be exactly
        # one task in `self.tasks.last` which the next line just
        # selects.
        last = list(self.tasks.last)[0]
        for task in self.tasks.values():
            task.dataset = self
            cls = task.__class__
            versioned = type(
                f"{self.name[0].upper()}{self.name[1:]} (versioned)",
                (cls,),
                {
                    "execute": self.check_version(
                        after_execution=[self.update] if task is last else []
                    )
                },
            )
            task.__class__ = versioned

        predecessors = [
            task
            for dataset in self.dependencies
            if isinstance(dataset, Dataset)
            for task in dataset.tasks.last
        ] + [task for task in self.dependencies if isinstance(task, Operator)]
        for p in predecessors:
            for first in self.tasks.first:
                p.set_downstream(first)

        self.register()

    def __init_subclass__(cls) -> None:
        # Warn about missing or invalid class attributes
        if not isinstance(getattr(cls, "sources", None), DatasetSources):
            logger.warning(
                f"Dataset '{cls.__name__}' does not define a valid class-level 'sources'.",
                stacklevel=2
            )
        if not isinstance(getattr(cls, "targets", None), DatasetTargets):
            logger.warning(
                f"Dataset '{cls.__name__}' does not define a valid class-level 'targets'.",
                stacklevel=2
            )

    def register(self):
        with db.session_scope() as session:
            existing = session.query(Model).filter_by(
                name=self.name
            ).first()

            if not existing:
                entry = Model(
                    name=self.name,
                    version="will be filled after execution",
                    scenarios="{}",
                    sources=self.sources.to_dict(),
                    targets=self.targets.to_dict()
                )
                session.add(entry)

def load_sources_and_targets(
    name: str,
) -> tuple[DatasetSources, DatasetTargets]:
    """
    Load DatasetSources and DatasetTargets from the datasets table.

    Parameters
    ----------
        name (str): Name of the dataset.

    Returns
    -------
        Tuple[DatasetSources, DatasetTargets]
    """
    with db.session_scope() as session:
        dataset_entry = (
            session.query(Model)
            .filter_by(name=name)
            .first()
        )

        if dataset_entry is None:
            raise ValueError(f"Dataset '{name}' not found in the database.")

        # Extract raw JSON dicts within the session
        raw_sources = dict(dataset_entry.sources or {})
        raw_targets = dict(dataset_entry.targets or {})

    # Recreate objects *outside the session* (now safe)
    sources = DatasetSources(**raw_sources)
    targets = DatasetTargets(**raw_targets)

    return sources, targets


def export_dataset_io_to_json(
    output_path: str = "dataset_io_overview.json",
) -> None:
    """
    Export all sources and targets of datasets to a JSON file.

    Parameters
    ----------
    output_path : str
        Path to the output JSON file.
    """

    result = {}

    with db.session_scope() as session:
        datasets = session.query(Model).all()

        for dataset in datasets:
            name = dataset.name

            try:
                raw_sources = dict(dataset.sources or {})
                raw_targets = dict(dataset.targets or {})

                result[name] = {
                    "sources": raw_sources,
                    "targets": raw_targets,
                }
            except Exception as e:
                print(f"⚠️ Could not process dataset '{name}': {e}")

    # Save to JSON
    output_file = Path(output_path)
    output_file.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"✅ Dataset I/O overview written to {output_file.resolve()}")
