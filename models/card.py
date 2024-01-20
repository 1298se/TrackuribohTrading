from typing import List

from sqlalchemy import Integer, String, ForeignKey, Text
from sqlalchemy.orm import relationship, mapped_column

from models import Base
from tasks.types import CardExtendedData, CardResponse


class Card(Base):
    __tablename__ = "card"

    id = mapped_column(Integer, primary_key=True)
    # Blue-Eyes White Dragon
    name = mapped_column(String(255), index=True)
    # name but without hyphens, semicolons, etc
    clean_name = mapped_column(String(255), index=True)
    image_url = mapped_column(String(255))
    set_id = mapped_column(Integer, ForeignKey("set.id"), nullable=False)
    set = relationship("Set", back_populates="cards")
    number = mapped_column(String(255), index=True)
    # Ultra rare
    # For some reason, the catalog endpoint for fetching rarities doesn't give us all possible card rarities...
    rarity_name = mapped_column(String(255), index=True)
    # LIGHT
    attribute = mapped_column(String(255))
    # Normal Monster
    card_type = mapped_column(String(255))
    # Dragon
    monster_type = mapped_column(String(255))
    attack = mapped_column(String(255))
    defense = mapped_column(String(255))
    description = mapped_column(Text)
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


# Not fetching card sale data for now...
# # Define the trigger as a raw SQL statement
# trigger = DDL("""
# CREATE OR REPLACE FUNCTION insert_card_sync_data()
# RETURNS TRIGGER AS $$
# BEGIN
#     -- Insert with a default sync frequency of 'LOW'
#     INSERT INTO card_sync_data (card_id, sync_frequency)
#     VALUES (NEW.id, 1);
#     RETURN NEW;
# END;
# $$ LANGUAGE plpgsql;
#
# CREATE TRIGGER insert_card_sync_data_trigger
# AFTER INSERT ON card
# FOR EACH ROW EXECUTE FUNCTION insert_card_sync_data();
# """)
#
#
# # Attach the trigger to the Card table
# event.listen(
#     Card.__table__,
#     'after_create',
#     trigger.execute_if(dialect='postgresql')
# )
