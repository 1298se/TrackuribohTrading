from sqlalchemy import Column, Integer, Boolean, String, Numeric, ForeignKey, DateTime, ForeignKeyConstraint, Index
from sqlalchemy.orm import relationship, column_property

from models import Base
from tasks.custom_types import SKUListingResponse


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

    total_price = column_property(price + seller_shipping_price)

    @staticmethod
    def from_tcgplayer_response(response: SKUListingResponse, timestamp):
        return SKUListing(
            id=response['listingId'],
            timestamp=timestamp,
            sku_id=response['productConditionId'],
            verified_seller=response['verifiedSeller'],
            gold_seller=response['goldSeller'],
            quantity=response['quantity'],
            seller_name=response['sellerName'],
            price=response['price'],
            seller_shipping_price=response['sellerShippingPrice'],
        )


sku_listing_sku_id_idx = Index("sku_listing_sku_id_idx", SKUListing.sku_id)
