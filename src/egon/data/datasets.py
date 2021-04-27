"""The API for configuring datasets.

This is an internal module. Most of the functionality is supposed to be
reexported from :py:mod:`egon.data` and should be used from there. If
you need any functionality from this module which is not reexported from
:py:mod:`egon.data`, this is considered a bug, so please file one,
requesting an addition of the needed functionality to the public API.
"""

from sqlalchemy import Column, ForeignKey, Integer, String, Table, orm
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db

Base = declarative_base()
SCHEMA = "metadata"
db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")

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
    schema="metadata",
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


Model.__table__.create(bind=db.engine(), checkfirst=True)
DependencyGraph.create(bind=db.engine(), checkfirst=True)
