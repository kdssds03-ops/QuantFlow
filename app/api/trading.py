"""
app.api.trading — 트레이딩 관련 엔드포인트
Celery 태스크를 트리거하고 상태를 조회하는 REST API
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# ⚠️ [순환 참조 가드] worker.tasks는 최상단 글로벌 레벨에서 임포트하지 않음.
# worker.tasks 모듈은 초기화 시 core/database, celery_app 등을 연쇄 로드하므로
# 모듈 레벨 임포트 시 FastAPI 초기화 순서와 충돌하여 ImportError/순환 참조 발생.
# → 태스크를 실제로 호출하는 각 함수 내부에서 지연 임포트(Lazy Import)로 처리.

router = APIRouter(prefix="/trading")


# ── Schemas ──────────────────────────────────
class TradeRequest(BaseModel):
    # 방향·수량은 전략(analyze_and_trade)이 결정한다 — 요청은 심볼만 받는다.
    symbol: str = "BTC/USDT"


class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str


# ── Endpoints ────────────────────────────────
@router.post("/order", response_model=TaskResponse)
async def create_order(req: TradeRequest):
    """
    해당 심볼의 분석·매매 태스크(analyze_and_trade)를 즉시 1회 트리거.
    즉시 task_id를 반환하고 비동기로 실행.
    """
    # [순환 참조 방어 가드] 함수 호출 시점에 지연 임포트
    from worker.tasks import analyze_and_trade

    task = analyze_and_trade.delay(symbol=req.symbol)
    return TaskResponse(
        task_id=task.id,
        status="queued",
        message=f"{req.symbol} 분석·매매 태스크가 큐에 등록됨",
    )


@router.get("/order/{task_id}", response_model=TaskResponse)
async def get_order_status(task_id: str):
    """Celery 태스크 상태 조회"""
    from celery.result import AsyncResult

    result = AsyncResult(task_id)
    return TaskResponse(
        task_id=task_id,
        status=result.status,
        message=str(result.result) if result.result else "처리 중...",
    )


@router.post("/market-data", response_model=TaskResponse)
async def fetch_market_data(symbol: str = "BTC/USDT"):
    """시세 데이터 수집 태스크 트리거"""
    # [순환 참조 방어 가드] 함수 호출 시점에 지연 임포트
    from worker.tasks import fetch_market_data_task

    task = fetch_market_data_task.delay(symbol=symbol)
    return TaskResponse(
        task_id=task.id,
        status="queued",
        message=f"{symbol} 시세 수집 태스크 등록됨",
    )
