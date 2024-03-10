from sqlalchemy import Column, Integer, Boolean, String, Numeric, ForeignKey, DateTime, ForeignKeyConstraint
from sqlalchemy.orm import relationship, column_property

from models import Base
from tasks.custom_types import SKUListingResponse

class SKUMaxProfit(Base):
    __tablename__ = 'sku_max_profit'

    sku_id = Column(Integer, ForeignKey('sku.id'), primary_key=True)
    sku = relationship("SKU")
    max_profit= Column(Numeric(precision=10, scale=2))
    num_cards = Column(Integer)
    cost = Column(Numeric(precision=10, scale=2))
