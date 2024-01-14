from sqlalchemy import SmallInteger, Column, String

from models import Base
from tasks.types import ConditionResponse


class Condition(Base):
    __tablename__ = "condition"

    id = Column(SmallInteger, primary_key=True)
    name = Column(String(255), unique=True, index=True)
    abbreviation = Column(String(255))
    order = Column(SmallInteger)

    @staticmethod
    def from_tcgplayer_response(response: ConditionResponse):
        return Condition(
            id=response['conditionId'],
            name=response['name'],
            abbreviation=response['abbreviation'],
            order=response['displayOrder'],
        )
