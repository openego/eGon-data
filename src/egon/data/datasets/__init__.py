"""The API for configuring datasets."""

from collections import abc
from dataclasses import dataclass
from typing import List, Set, Tuple, Union

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators import BaseOperator as Operator
from airflow.utils.dates import days_ago
from sqlalchemy import Column, ForeignKey, Integer, String, Table, orm
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db

Base = declarative_base()
SCHEMA = "metadata"
DEFAULTS = {"start_date": days_ago(1)}


def setup():
    """Create the database structure for storing dataset information."""
    # TODO: Move this into a task generating the initial database structure.
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
    Model.__table__.create(bind=db.engine(), checkfirst=True)
    DependencyGraph.create(bind=db.engine(), checkfirst=True)


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
    dependencies = orm.relationship(
        "Model",
        secondary=DependencyGraph,
        primaryjoin=id == DependencyGraph.c.dependent_id,
        secondaryjoin=id == DependencyGraph.c.dependency_id,
        backref="dependents",
    )


TaskGraph = Union[Operator, "ParallelTasks", "SequentialTasks"]
ParallelTasks = Set[TaskGraph]
SequentialTasks = Tuple[TaskGraph, ...]


@dataclass
class Tasks:
    first: Set[Operator]
    last: Set[Operator]
    all: Set[Operator]


def connect(tasks: TaskGraph):
    """Connect multiple tasks into a potentially complex graph.

    As per the type, a task graph can be given as a single operator, a tuple
    of task graphs or a set of task graphs. A tuple will be executed in the
    specified order, whereas a set means that the tasks in the graph will be
    executed in parallel.
    """
    if isinstance(tasks, Operator):
        return Tasks(first={tasks}, last={tasks}, all={tasks})
    elif isinstance(tasks, abc.Sized) and len(tasks) == 0:
        return Tasks(first={}, last={}, all={})
    elif isinstance(tasks, abc.Set):
        results = [connect(subtasks) for subtasks in tasks]
        first = {task for result in results for task in result.first}
        last = {task for result in results for task in result.last}
        tasks = {task for result in results for task in result.all}
        return Tasks(first, last, tasks)
    elif isinstance(tasks, tuple):
        results = [connect(subtasks) for subtasks in tasks]
        for (left, right) in zip(results[:-1], results[1:]):
            for last in left.last:
                for first in right.first:
                    last.set_downstream(first)
        first = results[0].first
        last = results[-1].last
        tasks = {task for result in results for task in result.all}
        return Tasks(first, last, tasks)
    else:
        raise (
            TypeError(
                "`egon.data.datasets.connect` got an argument of type:\n\n"
                f"  {type(tasks)}\n\n"
                "where only `Operator`s, `Set`s and `Tuple`s are allowed."
            )
        )


@dataclass
class Dataset:
    name: str
    version: str
    dependencies: List["Dataset"]
    graph: TaskGraph

    def check_version(self):
        def skip_task(task, *xs, **ks):
            raise AirflowSkipException(
                f"{self.name} version {self.version} already executed."
            )

        return skip_task

    def __post_init__(self):
        self.tasks = connect(self.graph)
        for task in self.tasks.all:
            cls = task.__class__
            versioned = type(
                f"{self.name}VersionCheck",
                (cls,),
                {"execute": self.check_version()},
            )
            task.__class__ = versioned

        predecessors = [p for d in self.dependencies for p in d.tasks.last]
        for p in predecessors:
            for first in self.tasks.first:
                p.set_downstream(first)

    def insert_into(self, dag: DAG):
        for task in self.tasks.all:
            for attribute in DEFAULTS:
                if getattr(task, attribute) is None:
                    setattr(task, attribute, DEFAULTS[attribute])
        dag.add_tasks(self.tasks.all)
