from enum import Enum

from sqlalchemy import Column, Integer, ForeignKey, Enum as SQLEnum

from models import Base


class SyncFrequency(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


class CardSyncData(Base):
    __tablename__ = 'card_sync_data'

    card_id = Column(Integer, ForeignKey('card.id'), primary_key=True)
    sync_frequency = Column(SQLEnum(SyncFrequency))
