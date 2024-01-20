from sqlalchemy import Column, SmallInteger, String

from models import Base
from tasks.custom_types import PrintingResponse


class Printing(Base):
    __tablename__ = "printing"

    id = Column(SmallInteger, primary_key=True)
    name = Column(String(255), unique=True, index=True)
    order = Column(SmallInteger)

    @staticmethod
    def from_tcgplayer_response(response: PrintingResponse):
        return Printing(
            id=response['printingId'],
            name=response['name'],
            order=response['displayOrder'],
        )
