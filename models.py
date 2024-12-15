# coding: utf-8
from sqlalchemy import Column, Float, ForeignKey, Integer, Table, Text
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Aggregator(Base):
    __tablename__ = 'aggregators'

    aggregator_id = Column(Integer, primary_key=True)
    guid = Column(Text, nullable=False)
    name = Column(Text, nullable=False)


class MetricSnapshot(Base):
    __tablename__ = 'metric_snapshots'

    metric_snapshot_id = Column(Integer, primary_key=True)
    device_id = Column(Integer, nullable=False)
    client_utc_timestamp_epoch = Column(Integer, nullable=False)
    client_timezone_mins = Column(Integer, nullable=False)
    server_utc_timestamp_epoch = Column(Integer, nullable=False)
    server_timezone_mins = Column(Integer, nullable=False)


t_sqlite_sequence = Table(
    'sqlite_sequence', metadata,
    Column('name', NullType),
    Column('seq', NullType)
)


class Device(Base):
    __tablename__ = 'devices'

    device_id = Column(Integer, primary_key=True)
    aggregator_id = Column(ForeignKey('aggregators.aggregator_id'), nullable=False)
    name = Column(Text, nullable=False)
    ordinal = Column(Integer, nullable=False)

    aggregator = relationship('Aggregator')


class DeviceMetricType(Base):
    __tablename__ = 'device_metric_types'

    device_metric_type_id = Column(Integer, primary_key=True)
    device_id = Column(ForeignKey('devices.device_id'), nullable=False)
    name = Column(Text, nullable=False)

    device = relationship('Device')


class MetricValue(Base):
    __tablename__ = 'metric_values'

    metric_snapshot_id = Column(ForeignKey('metric_snapshots.metric_snapshot_id'), primary_key=True, nullable=False)
    device_metric_type_id = Column(ForeignKey('device_metric_types.device_metric_type_id'), primary_key=True, nullable=False)
    value = Column(Float, nullable=False)

    device_metric_type = relationship('DeviceMetricType')
    metric_snapshot = relationship('MetricSnapshot')
