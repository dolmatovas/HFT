from dataclasses import dataclass
from typing import Optional


@dataclass
class Order:  # Our own placed order
    order_id: int
    side: str
    size: float
    price: float


@dataclass
class AnonTrade:  # Market trade
    timestamp: float
    side: str
    size: float
    price: str


@dataclass
class OwnTrade:  # Execution of own placed order
    timestamp: float
    trade_id: int
    order_id: int
    side: str
    size: float
    price: float


@dataclass
class OrderbookSnapshotUpdate:  # Orderbook tick snapshot
    timestamp: float
    asks: list[tuple[float, float]]  # tuple[price, size]
    bids: list[tuple[float, float]]


@dataclass
class MdUpdate:  # Data of a tick
    orderbook: Optional[OrderbookSnapshotUpdate] = None
    trades: Optional[list[AnonTrade]] = None


class Strategy:
    def __init__(self, max_position: float) -> None:
        pass

    def run(self, sim: "Sim"):
        while True:
            try:
                md_update = sim.tick()
                #call sim.place_order and sim.cancel_order here
            except StopIteration:
                break


def load_md_from_file(path: str) -> list[MdUpdate]:
    # TODO: load actual md
    return list(map(lambda _: MdUpdate(), range(10**3)))


class Sim:
    def __init__(self, execution_latency: float, md_latency: float) -> None:
        self.md = iter(load_md_from_file("./random/location/md.csv"))

    def tick(self) -> MdUpdate:
        self.execute_orders()
        self.prepare_orders()

        return next(self.md)

    def prepare_orders(self):
        pass

    def execute_orders(self):
        pass

    def place_order(self):
        pass

    def cancel_order(self):
        pass



if __name__ == "__main__":
    strategy = Strategy(10)
    sim = Sim(10, 10)
    strategy.run(sim)
