"""
app.models — SQLAlchemy ORM 모델 패키지

이 파일을 임포트하면 모든 모델이 Base.metadata에 등록됩니다.
"""

from core.database import Base

# 모델 임포트 (Base.metadata 등록)
from app.models.models import MarketData, TradeHistory  # noqa: F401

__all__ = ["Base", "MarketData", "TradeHistory"]
