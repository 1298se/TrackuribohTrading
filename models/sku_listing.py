from sqlalchemy import Column, Integer, Boolean, String, Numeric, ForeignKey, DateTime
from sqlalchemy.orm import relationship

from models import Base
from jobs.types import SKUListingResponse


class SKUListing(Base):
    __tablename__ = 'sku_listing'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, primary_key=True)
    sku_id = Column(Integer, ForeignKey('sku.id'), nullable=False)
    sku = relationship("SKU")
    verified_seller = Column(Boolean)
    gold_seller = Column(Boolean)
    quantity = Column(Integer)
    seller_name = Column(String)
    price = Column(Numeric(precision=10, scale=2))  # Numeric for price
    seller_shipping_price = Column(Numeric(precision=10, scale=2))  # Numeric for seller_shipping_price

    @staticmethod
    def from_tcgplayer_response(response: SKUListingResponse, timestamp, sku_id):
        return SKUListing(
            id=response['listingId'],
            timestamp=timestamp,
            sku_id=sku_id,
            verified_seller=response['verifiedSeller'],
            gold_seller=response['goldSeller'],
            quantity=response['quantity'],
            seller_name=response['sellerName'],
            price=response['price'],
            seller_shipping_price=response['sellerShippingPrice'],
        )
