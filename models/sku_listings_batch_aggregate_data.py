from sqlalchemy import ForeignKey, Integer, Column, Numeric, DateTime
from sqlalchemy.orm import relationship

from models import Base


class SKUListingsBatchAggregateData(Base):
    __tablename__ = 'sku_listings_batch_aggregate_data'

    sku_id = Column(Integer, ForeignKey('sku.id'), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)
    sku = relationship("SKU")
    lowest_listing_price = Column(Numeric(precision=10, scale=2))  # Numeric for price
    total_listings_count = Column(Integer)
    total_copies_count = Column(Integer)
