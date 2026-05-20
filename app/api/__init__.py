"""
app.api — API 라우터 패키지
"""

from fastapi import APIRouter

from app.api.routes import router as health_router
from app.api.trading import router as trading_router

router = APIRouter()
router.include_router(health_router, tags=["system"])
router.include_router(trading_router, tags=["trading"])
