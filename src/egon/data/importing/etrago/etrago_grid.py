# coding: utf-8
from sqlalchemy import ARRAY, BigInteger, Boolean, CHAR, Column, Date, DateTime, Float, ForeignKey, Integer, JSON, Numeric, SmallInteger, String, Table, Text, text
from geoalchemy2.types import Geometry
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata



class EgonPfHvBus(Base):
    __tablename__ = 'egon_pf_hv_bus'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    bus_id = Column(BigInteger, primary_key=True, nullable=False)
    v_nom = Column(Float(53))
    carrier = Column(Text)
    v_mag_pu_min = Column(Float(53))
    v_mag_pu_max = Column(Float(53))
    geom = Column(Geometry('POINT', 4326), index=True)


class EgonPfHvGenerator(Base):
    __tablename__ = 'egon_pf_hv_generator'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    generator_id = Column(BigInteger, primary_key=True, nullable=False)
    bus = Column(BigInteger)
    control = Column(Text)
    carrier = Column(Text)
    p_nom = Column(Float(53))
    p_nom_extendable = Column(Boolean)
    p_nom_min = Column(Float(53))
    p_nom_max = Column(Float(53))
    p_min_pu_fixed = Column(Float(53))
    p_max_pu_fixed = Column(Float(53))
    sign = Column(Float(53))
    marginal_cost_fixed = Column(Float(53))
    capital_cost = Column(Float(53))
    efficiency = Column(Float(53))
    committable = Column(Boolean)
    start_up_cost = Column(Float(53))
    shut_down_cost = Column(Float(53))
    min_up_time = Column(BigInteger)
    min_down_time = Column(BigInteger)
    up_time_before = Column(BigInteger)    
    down_time_before = Column(BigInteger)    
    ramp_limit_up = Column(Float(53))
    ramp_limit_down = Column(Float(53))
    ramp_limit_start_up = Column(Float(53))
    ramp_limit_shut_down = Column(Float(53))


class EgonPfHvGeneratorTimeseries(Base):
    __tablename__ = 'egon_pf_hv_generator_timeseries'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    generator_id = Column(Integer, primary_key=True, nullable=False)
    temp_id = Column(Integer, nullable=False)
    p_set = Column(ARRAY(Float(precision=53)))
    q_set = Column(ARRAY(Float(precision=53)))
    p_min_pu = Column(ARRAY(Float(precision=53)))
    p_max_pu = Column(ARRAY(Float(precision=53)))
    marginal_cost = Column(ARRAY(Float(precision=53)))


class EgonPfHvLine(Base):
    __tablename__ = 'egon_pf_hv_line'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    line_id = Column(BigInteger, primary_key=True, nullable=False)
    bus0 = Column(BigInteger)
    bus1 = Column(BigInteger)
    carrier = Column(Text)    
    x = Column(Numeric)
    r = Column(Numeric)
    g = Column(Numeric)
    b = Column(Numeric)
    s_nom = Column(Numeric)
    s_nom_extendable = Column(Boolean)
    s_nom_min = Column(Float(53))
    s_nom_max = Column(Float(53))
    s_max_pu_fixed = Column(Float(53))
    capital_cost = Column(Float(53))
    length = Column(Float(53))
    cables = Column(Integer)
    terrain_factor = Column(Float(53))
    geom = Column(Geometry('MULTILINESTRING', 4326))
    topo = Column(Geometry('LINESTRING', 4326))


class EgonPfHvLineTimeseries(Base):
    __tablename__ = 'egon_pf_hv_line_timeseries'
    __table_args__ = {'schema': 'grid'}

    version = version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    line_id = Column(BigInteger, primary_key=True, nullable=False)
    s_max_pu = Column(ARRAY(Float(precision=53)))


class EgonPfHvLink(Base):
    __tablename__ = 'egon_pf_hv_link'
    __table_args__ = {'schema': 'grid'}

    version = version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    link_id = Column(BigInteger, primary_key=True, nullable=False)
    bus0 = Column(BigInteger)
    bus1 = Column(BigInteger)
    carrier = Column(Text)
    efficiency_fixed = Column(Float(53))
    p_nom = Column(Numeric)
    p_nom_extendable = Column(Boolean)
    p_nom_min = Column(Float(53))
    p_nom_max = Column(Float(53))
    p_min_pu_fixed = Column(Float(53))
    p_max_pu_fixed = Column(Float(53))
    p_set_fixed = Column(Float(53))
    capital_cost = Column(Float(53))
    marginal_cost_fixed = Column(Float(53))
    length = Column(Float(53))
    terrain_factor = Column(Float(53))
    geom = Column(Geometry('MULTILINESTRING', 4326))
    topo = Column(Geometry('LINESTRING', 4326))

class EgonPfHvLinkTimeseries(Base):
    __tablename__ = 'egon_pf_hv_link_timeseries'
    __table_args__ = {'schema': 'grid'}

    version = version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    link_id = Column(BigInteger, primary_key=True, nullable=False)
    p_set = Column(ARRAY(Float(precision=53)))
    p_min_pu = Column(ARRAY(Float(precision=53)))
    p_max_pu = Column(ARRAY(Float(precision=53)))
    efficiency = Column(ARRAY(Float(precision=53)))
    marginal_cost = Column(ARRAY(Float(precision=53)))


class EgonPfHvLoad(Base):
    __tablename__ = 'egon_pf_hv_load'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    load_id = Column(BigInteger, primary_key=True, nullable=False)
    bus = Column(BigInteger)
    carrier = Column(Text)
    sign = Column(Float(53))


class EgonPfHvLoadTimeseries(Base):
    __tablename__ = 'egon_pf_hv_load_timeseries'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    load_id = Column(BigInteger, primary_key=True, nullable=False)
    temp_id = Column(Integer, nullable=False)
    p_set = Column(ARRAY(Float(precision=53)))
    q_set = Column(ARRAY(Float(precision=53)))


class EgonPfHvCarrier(Base):
    __tablename__ = 'egon_pf_hv_carrier'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    name = Column(Text)
    co2_emissions = Column(Float(53))
    commentary = Column(Text)


class EgonPfHvStorage(Base):
    __tablename__ = 'egon_pf_hv_storage'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    storage_id = Column(BigInteger, primary_key=True, nullable=False)
    bus = Column(BigInteger)
    control = Column(Text)
    carrier = Column(Text)
    p_nom = Column(Float(53))
    p_nom_extendable = Column(Boolean)
    p_nom_min = Column(Float(53))
    p_nom_max = Column(Float(53))
    p_min_pu_fixed = Column(Float(53))
    p_max_pu_fixed = Column(Float(53))
    sign = Column(Float(53))
    marginal_cost_fixed = Column(Float(53))
    capital_cost = Column(Float(53))
    state_of_charge_initial = Column(Float(53))
    cyclic_state_of_charge = Column(Boolean)
    max_hours = Column(Float(53))
    efficiency_store = Column(Float(53))
    efficiency_dispatch = Column(Float(53))
    standing_loss = Column(Float(53))


class EgonPfHvStorageTimeseries(Base):
    __tablename__ = 'egon_pf_hv_storage_timeseries'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    storage_id = Column(BigInteger, primary_key=True, nullable=False)
    temp_id = Column(Integer, primary_key=True, nullable=False)
    p_set = Column(ARRAY(Float(precision=53)))
    q_set = Column(ARRAY(Float(precision=53)))
    p_min_pu = Column(ARRAY(Float(precision=53)))
    p_max_pu = Column(ARRAY(Float(precision=53)))
    state_of_charge_set = Column(ARRAY(Float(precision=53)))
    inflow = Column(ARRAY(Float(precision=53)))
    marginal_cost = Column(ARRAY(Float(precision=53)))

class EgonPfHvStore(Base):
    __tablename__ = 'egon_pf_hv_store'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    store_id = Column(BigInteger, primary_key=True, nullable=False)
    bus = Column(BigInteger)
    carrier = Column(Text)
    e_nom = Column(Float(53))
    e_nom_extendable = Column(Boolean)
    e_nom_min = Column(Float(53))
    e_nom_max = Column(Float(53))
    e_min_pu_fixed = Column(Float(53))
    e_max_pu_fixed = Column(Float(53))
    e_initial = Column(Float(53))
    e_cyclic = Column(Boolean)
    sign = Column(Float(53))
    marginal_cost_fixed = Column(Float(53))
    capital_cost = Column(Float(53))
    standing_loss = Column(Float(53))

class EgonPfHvStoreTimeseries(Base):
    __tablename__ = 'egon_pf_hv_store_timeseries'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    store_id = Column(BigInteger, primary_key=True, nullable=False)
    temp_id = Column(Integer, primary_key=True, nullable=False)
    p_set = Column(ARRAY(Float(precision=53)))
    q_set = Column(ARRAY(Float(precision=53)))
    e_min_pu = Column(ARRAY(Float(precision=53)))
    e_max_pu = Column(ARRAY(Float(precision=53)))
    marginal_cost = Column(ARRAY(Float(precision=53)))

class EgonPfHvTempResolution(Base):
    __tablename__ = 'egon_pf_hv_temp_resolution'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    temp_id = Column(BigInteger, primary_key=True, nullable=False)
    timesteps = Column(BigInteger, nullable=False)
    resolution = Column(Text)
    start_time = Column(DateTime)


class EgonPfHvTransformer(Base):
    __tablename__ = 'egon_pf_hv_transformer'
    __table_args__ = {'schema': 'grid'}

    version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    trafo_id = Column(BigInteger, primary_key=True, nullable=False)
    bus0 = Column(BigInteger)
    bus1 = Column(BigInteger)
    model = Column(Text)
    x = Column(Numeric)
    r = Column(Numeric)
    g = Column(Numeric)
    b = Column(Numeric)
    s_nom = Column(Float(53))
    s_nom_extendable = Column(Boolean)
    s_nom_min = Column(Float(53))
    s_nom_max = Column(Float(53))
    s_max_pu_fixed = Column(Float(53))
    tap_ratio = Column(Float(53))
    tap_side = Column(BigInteger)
    tap_position = Column(BigInteger)
    phase_shift = Column(Float(53))
    v_ang_min = Column(Float(53))
    v_ang_max = Column(Float(53))
    capital_cost = Column(Float(53))
    num_parallel = Column(Float(53))
    geom = Column(Geometry('MULTILINESTRING', 4326))
    topo = Column(Geometry('LINESTRING', 4326))


class EgonPfHvTransformerTimeseries(Base):
    __tablename__ = 'egon_pf_hv_transformer_timeseries'
    __table_args__ = {'schema': 'grid'}

    version = version = Column(Text, primary_key=True, nullable=False)
    scn_name = Column(String, primary_key=True, nullable=False)
    trafo_id = Column(BigInteger, primary_key=True, nullable=False)
    s_max_pu = Column(ARRAY(Float(precision=53)))

