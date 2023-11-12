from typing import TypedDict, List


class CardListingRequestData(TypedDict):
    product_id: int
    condition: str
    printing: str
    sku_id: int


class SKUListingResponse(TypedDict):
    listingId: float
    productConditionId: float
    verifiedSeller: bool
    goldSeller: bool
    quantity: float
    sellerName: str
    sellerShippingPrice: float
    price: float
    shippingPrice: float


class CardSaleResponse(TypedDict):
    condition: str
    variant: str
    language: str
    quantity: int
    title: str
    listingType: str
    customListingId: str
    purchasePrice: float
    shippingPrice: float
    orderDate: str


class CardSetResponse(TypedDict):
    groupId: int
    name: str
    abbreviation: str
    isSupplemental: bool
    publishedOn: str
    modifiedOn: str
    categoryId: int


class PrintingResponse(TypedDict):
    printingId: int
    name: str
    displayOrder: int
    modifiedOn: str


class ConditionResponse(TypedDict):
    conditionId: int
    name: str
    abbreviation: str
    displayOrder: int


class RarityResponse(TypedDict):
    rarityId: int
    displayText: str
    dbValue: str


class SKUResponse(TypedDict):
    skuId: int
    productId: int
    languageId: int
    printingId: int
    conditionId: int


class CardExtendedData(TypedDict):
    name: str
    value: str


class CardResponse(TypedDict):
    productId: int
    name: str
    cleanName: str
    imageUrl: str
    categoryId: int
    groupId: int
    url: str
    modifiedOn: str
    skus: List[SKUResponse]
    imageCount: int
    presaleInfo: dict
    extendedData: List[CardExtendedData]


class CardSalesResponse(TypedDict):
    nextPage: str
    data: List[CardSaleResponse]
