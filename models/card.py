from typing import List

from sqlalchemy import Column, Integer, String, ForeignKey, SmallInteger, Text
from sqlalchemy.orm import relationship, Session

from models import Base
from jobs.types import CardExtendedData, CardResponse


class Card(Base):
    __tablename__ = "card"

    id = Column(Integer, primary_key=True)
    # Blue-Eyes White Dragon
    name = Column(String(255), index=True)
    # name but without hyphens, semicolons, etc
    clean_name = Column(String(255), index=True)
    image_url = Column(String(255))
    set_id = Column(Integer, ForeignKey("set.id"), nullable=False)
    set = relationship("Set", back_populates="cards")
    number = Column(String(255), index=True)
    # Ultra rare
    # For some reason, the catalog endpoint for fetching rarities doesn't give us all possible card rarities...
    rarity_name = Column(String(255), index=True)
    # LIGHT
    attribute = Column(String(255))
    # Normal Monster
    card_type = Column(String(255))
    # Dragon
    monster_type = Column(String(255))
    attack = Column(String(255))
    defense = Column(String(255))
    description = Column(Text)
    skus = relationship("SKU", back_populates="card")
    sales = relationship("CardSale", back_populates="card")

    @staticmethod
    def parse_extended_data(extended_data: List[CardExtendedData]) -> dict:
        return {data['name']: data['value'] for data in extended_data}

    @staticmethod
    def from_tcgplayer_response(response: CardResponse):
        card_metadata = Card.parse_extended_data(response['extendedData'])

        return Card(
            id=response['productId'],
            name=response['name'],
            clean_name=response['cleanName'],
            image_url=response['imageUrl'],
            set_id=response['groupId'],
            number=card_metadata.get('Number'),
            rarity_name=card_metadata.get('Rarity', None),
            attribute=card_metadata.get('Attribute'),
            card_type=card_metadata.get('Card Type'),
            monster_type=card_metadata.get('MonsterType'),
            attack=card_metadata.get('Attack'),
            defense=card_metadata.get('Defense'),
            description=card_metadata.get('Description'),
        )
