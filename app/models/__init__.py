"""
app.models — SQLAlchemy ORM 모델 패키지

이 파일을 임포트하면 모든 모델이 Base.metadata에 등록됩니다.
"""

import enum
from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Enum, Text,
    func,
)
from core.database import Base

# MarketData 모델 임포트 (Base.metadata 등록)
from app.models.models import MarketData  # noqa: F401

__all__ = [
    "Base",
    "MarketData",
    "TradeOrder",
    "OrderSide",
    "OrderStatus",
]


class OrderSide(str, enum.Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, enum.Enum):
    PENDING = "pending"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    FAILED = "failed"


class TradeOrder(Base):
    """거래 주문 기록"""
    __tablename__ = "trade_orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, index=True)
    side = Column(Enum(OrderSide), nullable=False)
    order_type = Column(String(10), nullable=False, default="market")
    amount = Column(Float, nullable=False)
    price = Column(Float, nullable=True)
    filled_price = Column(Float, nullable=True)
    status = Column(Enum(OrderStatus), nullable=False, default=OrderStatus.PENDING)
    exchange_order_id = Column(String(100), nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<TradeOrder {self.id} {self.side.value} {self.amount} {self.symbol}>"
