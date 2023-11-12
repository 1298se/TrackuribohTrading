from datetime import datetime
from typing import Optional, List

from sqlalchemy import DateTime
from sqlalchemy.orm import Session

from models.card_sale import CardSale


def get_most_recent_sale_timestamp_for_card(session: Session, card_id) -> Optional[datetime]:
    print("BRUH")
    most_recent_card_sale: CardSale = session.query(CardSale)\
        .filter(CardSale.card_id == card_id)\
        .order_by(CardSale.order_date.desc())\
        .first()

    print("BRUHHH", most_recent_card_sale)
    if most_recent_card_sale is None:
        return None

    return most_recent_card_sale.order_date
