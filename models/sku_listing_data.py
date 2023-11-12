from sqlalchemy import Column, Integer, DateTime, String, Float, ForeignKey
from sqlalchemy.orm import relationship

from models import Base


class SKUListingData(Base):
    __tablename__ = 'sku_listing_data'

    timestamp = Column(DateTime, primary_key=True)
    sku_id = Column(Integer, ForeignKey('sku.id'), primary_key=True, nullable=False)
    sku = relationship("SKU")
    lowest_price = Column(Float)
    copies_count = Column(Float)
    listings_count = Column(Integer)
