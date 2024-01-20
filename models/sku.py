from sqlalchemy import Integer, ForeignKey, SmallInteger
from sqlalchemy.orm import relationship, mapped_column

from models import Base
from tasks.custom_types import SKUResponse


class SKU(Base):
    __tablename__ = "sku"

    id = mapped_column(Integer, primary_key=True)
    card_id = mapped_column(Integer, ForeignKey('card.id'), nullable=False)
    card = relationship("Card", back_populates="skus")
    printing_id = mapped_column(SmallInteger, ForeignKey('printing.id'))
    printing = relationship("Printing")
    condition_id = mapped_column(SmallInteger, ForeignKey('condition.id'))
    condition = relationship("Condition")

    @staticmethod
    def from_tcgplayer_response(response: SKUResponse):
        return SKU(
            id=response['skuId'],
            card_id=response['productId'],
            printing_id=response['printingId'],
            condition_id=response.get('conditionId'),
        )
