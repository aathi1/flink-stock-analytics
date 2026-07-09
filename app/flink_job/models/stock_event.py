from dataclasses import dataclass


@dataclass
class StockEvent:

    symbol: str

    price: float

    timestamp: str