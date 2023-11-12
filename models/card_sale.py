from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Integer, String, Numeric, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from models import Base
from jobs.types import CardSaleResponse


class CardSale(Base):
    __tablename__ = 'card_sales'

    id = Column(Integer, primary_key=True)
    order_date = Column(DateTime(timezone=True))
    printing_name = Column(String(255), ForeignKey('printing.name'))
    printing = relationship("Printing")
    condition_name = Column(String(255), ForeignKey('condition.name'))
    condition = relationship("Condition")
    card_id = Column(Integer, ForeignKey('card.id'), nullable=False)
    card = relationship("Card", back_populates="sales")
    quantity = Column(Integer)
    listing_type = Column(String(50))
    purchase_price = Column(Numeric(precision=10, scale=2))  # Numeric type for prices
    shipping_price = Column(Numeric(precision=10, scale=2))  # Numeric type for prices

    @staticmethod
    def from_tcgplayer_response(response: CardSaleResponse, card_id: int):
        return CardSale(
            order_date=CardSale.parse_response_order_date(response['orderDate']),
            printing_name=response['variant'],
            condition_name=response['condition'],
            card_id=card_id,
            quantity=response['quantity'],
            listing_type=response['listingType'],
            purchase_price=response['purchasePrice'],
            shipping_price=response['shippingPrice'],
        )

    @staticmethod
    def parse_response_order_date(date: str) -> Optional[datetime]:
        # The time is either fraction or not fractional...
        # Format with fractional seconds
        format_with_fractions = '%Y-%m-%dT%H:%M:%S.%f%z'
        # Format without fractional seconds
        format_without_fractions = '%Y-%m-%dT%H:%M:%S%z'

        try:
            # Try to parse the date string with fractional seconds
            return datetime.strptime(date, format_with_fractions)
        except ValueError:
            try:
                # If that fails, try to parse the date string without fractional seconds
                return datetime.strptime(date, format_without_fractions)
            except ValueError:
                # If both attempts fail, return None
                raise "BRUH WHAT THE FUCK"
