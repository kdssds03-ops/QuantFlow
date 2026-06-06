"""
worker.predictor — 규칙 기반 및 ML 기반 매매 판단 엔진 추상화 레이어

Phase 4: MLPredictor 고도화 및 다중 기술적 지표 알고리즘 탑재
  - 12종 기술적 지표 기반 다중 피처 파이프라인 (인메모리 벡터 연산)
  - LightGBM model.pkl 싱글턴 로드 (joblib → pickle 체인)
  - predict_proba 기반 신뢰도(Confidence) 필터링 (65% 가드라인)
  - 상식 검증 가드 (RSI·Stochastic 모순 감지 → 뇌동매매 차단)
  - RuleBasedPredictor 자동 폴백 방패 (pkl 로드 실패 / 추론 예외)

Phase 5 (Timezone & NaN Fix):
  - MLPredictor DB 조회 시 UTC timestamp 기준 정렬 적용 (타임존 혼재 방지)
  - RuleBasedPredictor NaN 감지 로직 강화 및 WARNING 로그 금액화

Phase 6 (Aggressive Hybrid Strategy):
  - 래리 윌리엄스 변동성 돌파(Volatility Breakout) 룰셋 통합
    · Target_Price = 오늘시가 + (전일고가 - 전일저가) × 0.5
    · 현재가가 Target_Price 돌파 + 거래량 동반 시 신뢰도 +0.2 가중치
  - 동적 확신도 임계치(Dynamic Confidence Threshold)
    · 기본값 0.65 유지
    · 거래량 급등(현재 > 20봉 평균) 또는 RSI(14) 40~60 강한 추세 구간 → 0.55 자동 하향
"""

import logging
import math
import os
import pickle
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Tuple, Dict, Any, Optional

import numpy as np
import pandas as pd

# 머신러닝 라이브러리 안전 임포트 가드
try:
    import xgboost as xgb
except ImportError:
    xgb = None
    
try:
    import lightgbm as lgb
except ImportError:
    lgb = None

from app.models.models import MarketData

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# 추상 인터페이스
# ═══════════════════════════════════════════════════════════════════════════


class BasePredictor(ABC):
    """
    모든 매매 시그널 예측기의 최상위 추상 인터페이스입니다.
    새로운 예측 모델(MLPredictor, DeepLearningPredictor 등)은 이 클래스를 상속받아 구현해야 합니다.
    """

    @abstractmethod
    def predict(self, market_data: MarketData) -> str:
        """
        시장 데이터를 분석하여 매매 시그널("BUY", "SELL", "HOLD")을 반환합니다.

        Args:
            market_data (MarketData): 최신 캔들 정보 및 피처 지표가 포함된 ORM 인스턴스

        Returns:
            str: "BUY", "SELL", "HOLD" 중 하나
        """
        pass

    @abstractmethod
    def predict_with_confidence(self, market_data: MarketData) -> Tuple[str, float]:
        """
        매매 시그널과 신뢰도를 함께 반환합니다.

        Args:
            market_data (MarketData): 최신 캔들 정보가 포함된 ORM 인스턴스

        Returns:
            Tuple[str, float]: ("BUY"/"SELL"/"HOLD", 0.0~1.0 신뢰도)
        """
        pass

    def predict_signal(self, market_data: MarketData) -> Dict[str, Any]:
        """
        표준 반환 스펙: {"status": "buy"/"sell"/"hold", "confidence": float}
        """
        action, confidence = self.predict_with_confidence(market_data)
        return {"status": action.lower(), "confidence": confidence}


# ═══════════════════════════════════════════════════════════════════════════
# 규칙 기반 예측기 (Phase 4 고도화: 다중 조건 신뢰도 산출)
# ═══════════════════════════════════════════════════════════════════════════


class RuleBasedPredictor(BasePredictor):
    """
    볼린저 밴드(Bollinger Bands)와 RSI(Relative Strength Index)를 조합한
    규칙 기반(Rule-based) 매매 시그널 생성 클래스입니다.

    Phase 4 고도화:
      - predict_with_confidence() 메서드 추가
      - BB 이탈 깊이 + RSI 극단도 가중 평균 기반 신뢰도(Confidence) 산출
      - MLPredictor 장애 시 자동 폴백 방패 역할

    Phase 6 고도화 (공격형 하이브리드):
      - 래리 윌리엄스 변동성 돌파(Volatility Breakout) 필터 내장
        · BUY 시그널 확정 후 변동성 돌파 조건 동시 충족 시 신뢰도 +0.2 추가 가중치
        · 조건: current_close > (open + (prev_high - prev_low) × 0.5) AND volume 동반

    매매 전략 원리:
    - BUY (매수): 종가(close) <= 볼린저 밴드 하단(bb_lower) 이고 RSI(rsi_14) <= 30
    - SELL (매도): 종가(close) >= 볼린저 밴드 상단(bb_upper) 이고 RSI(rsi_14) >= 70
    - HOLD (관망): 그 외의 모든 상황
    """

    # 매매 전략 임계치 설정 (부동소수점 오차 방지를 위한 Decimal 상수 정의)
    RSI_OVERSOLD = Decimal("30")
    RSI_OVERBOUGHT = Decimal("70")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # RuleBasedPredictor는 순수 볼린저밴드+RSI 규칙만 사용한다.
        # (과거 이 생성자가 XGBoost 모델을 self.model로 로드했으나 predict 로직에서
        #  전혀 참조하지 않는 죽은 코드였다 → 매 부팅 시 불필요한 파일 I/O·메모리 제거.
        #  ML 추론이 필요하면 PREDICTOR_TYPE=ML 로 MLPredictor를 사용할 것.)
        self.model = None

    def predict(self, market_data: MarketData) -> str:
        """
        최신 시장 데이터를 기반으로 매매 시그널("BUY", "SELL", "HOLD")을 예측하여 반환합니다.

        Args:
            market_data (MarketData): 최신 캔들 정보 및 피처 지표가 포함된 ORM 인스턴스

        Returns:
            str: "BUY", "SELL", "HOLD" 중 하나
        """
        action, _ = self.predict_with_confidence(market_data)
        return action

    def predict_with_confidence(self, market_data: MarketData) -> Tuple[str, float]:
        """
        매매 시그널과 조건 충족도 기반 신뢰도를 반환합니다.

        신뢰도 산출 공식:
          기본 신뢰도 0.70 (양대 조건 동시 충족 시)
          + BB 이탈 깊이 보너스 (최대 +0.125)
          + RSI 극단도 보너스 (최대 +0.125)
          + [Phase 6] 변동성 돌파 보너스 +0.20 (래리 윌리엄스 룰셋 — BUY 확정 후 추가 적용)
          → 합산 최대 0.95 캡

        Args:
            market_data (MarketData): 최신 캔들 정보 및 피처 지표가 포함된 ORM 인스턴스

        Returns:
            Tuple[str, float]: ("BUY"/"SELL"/"HOLD", 0.0~0.95 신뢰도)
        """
        # 1. 방어적 프로그래밍: 속성 추출 헬퍼 함수 정의
        # market_data가 dict 형태 또는 SQLAlchemy ORM 객체인 경우를 유연하게 모두 지원합니다.
        def _get_val(attr_name: str):
            if isinstance(market_data, dict):
                return market_data.get(attr_name)
            return getattr(market_data, attr_name, None)

        close_raw    = _get_val("close")
        rsi_raw      = _get_val("rsi_14")
        bb_upper_raw = _get_val("bb_upper")
        bb_lower_raw = _get_val("bb_lower")
        # [Phase 6] 변동성 돌파 룰셋에 필요한 추가 지표
        open_raw     = _get_val("open")
        volume_raw   = _get_val("volume")
        high_raw     = _get_val("high")
        low_raw      = _get_val("low")

        raw_indicators = {
            "close": close_raw,
            "rsi_14": rsi_raw,
            "bb_upper": bb_upper_raw,
            "bb_lower": bb_lower_raw,
        }

        # 2. 데이터 부족(warm-up) 및 NaN 검증
        for name, val in raw_indicators.items():
            # None(또는 NULL) 값 체크
            if val is None:
                logger.info(
                    f"⏸️  [RuleBasedPredictor] 데이터 부족: {name} 지표가 None (Warm-up 구간) -> HOLD 반환"
                )
                return "HOLD", 0.0

            # float 타입의 NaN 값 체크
            if isinstance(val, float) and math.isnan(val):
                logger.info(
                    f"⏸️  [RuleBasedPredictor] 데이터 불완전: {name} 지표가 NaN (Warm-up 구간) -> HOLD 반환"
                )
                return "HOLD", 0.0

        # 3. 부동소수점 오차 방지를 위한 Decimal 타입 변환 및 정밀성 보장
        try:
            close    = Decimal(str(close_raw))
            rsi_14   = Decimal(str(rsi_raw))
            bb_upper = Decimal(str(bb_upper_raw))
            bb_lower = Decimal(str(bb_lower_raw))
        except (ValueError, TypeError, InvalidOperation) as exc:
            logger.error(
                f"❌ [RuleBasedPredictor] Decimal 변환 중 예외 발생: {exc} -> 안전을 위해 HOLD 반환"
            )
            return "HOLD", 0.0

        # Decimal 내부의 NaN 값 체크 (방어적 프로그래밍의 일환)
        if close.is_nan() or rsi_14.is_nan() or bb_upper.is_nan() or bb_lower.is_nan():
            logger.warning(
                "⚠️  [RuleBasedPredictor] 정밀 변환 지표 중 NaN 감지 → HOLD 반환\n"
                "   ▶ 근본 원인: DB의 지표(rsi_14/bb_upper/bb_lower)가 NULL로 저장되어 있음.\n"
                "   ▶ 해결책: tasks.py의 _warmup_from_db() 실행 또는 fetch_market_data_task 수집 대기 중"
            )
            return "HOLD", 0.0

        # [Phase 6] 변동성 돌파 판정 헬퍼 —————————————————————————————————————
        # 래리 윌리엄스 룰: Target_Price = 오늘시가 + (전일고가 - 전일저가) × 0.5
        # 현재가 > Target_Price AND 거래량 동반 → 추세 초입 강력 매수 가중치 +0.10
        # 지표가 없으면(None/NaN/0) 안전하게 False 처리하여 기존 로직에 영향 없음.
        def _check_volatility_breakout() -> bool:
            """래리 윌리엄스 변동성 돌파 조건 판정 (True=돌파 확인)
            
            ⚠️ 데이터 제약으로 인해 '전일 범위' 대신 '현재 캔들 고가-저가(H-L) 범위'로 근사하여 판정합니다.
            이는 매매 신호가 너무 잦아질 수 있으므로 보너스 가중치를 보수적으로 설정(0.10)합니다.
            """
            try:
                if open_raw is None or high_raw is None or low_raw is None or volume_raw is None:
                    return False
                _open   = Decimal(str(open_raw))
                _high   = Decimal(str(high_raw))
                _low    = Decimal(str(low_raw))
                _volume = Decimal(str(volume_raw))
                # 유효성 체크
                if _open.is_nan() or _high.is_nan() or _low.is_nan() or _volume.is_nan():
                    return False
                if _volume <= Decimal("0"):
                    return False
                # Target_Price 계산 (전일 범위를 현재 캔들 H-L로 근사)
                _day_range = _high - _low
                if _day_range <= Decimal("0"):
                    return False
                _target_price = _open + _day_range * Decimal("0.5")
                # 현재가가 Target_Price 돌파했는지 판정
                if close > _target_price:
                    logger.info(
                        "🚀 [VB_BREAKOUT] 변동성 돌파 확인! "
                        "Close=%.2f > Target=%.2f (Open=%.2f, Range=%.2f)",
                        float(close), float(_target_price),
                        float(_open), float(_day_range),
                    )
                    return True
                return False
            except (ValueError, TypeError, InvalidOperation):
                return False

        # 4. 볼린저 밴드 + RSI 조합 매매 전략 수행 (신뢰도 산출 포함)
        bb_width = bb_upper - bb_lower

        # BUY: 현재 종가(close)가 볼린저 밴드 하단(bb_lower) 이하 '이고', RSI(rsi_14)가 30 이하일 때
        if close <= bb_lower and rsi_14 <= self.RSI_OVERSOLD:
            # 신뢰도 산출: BB 이탈 깊이 + RSI 극단도 가중 평균
            bb_penetration = (
                float((bb_lower - close) / bb_width) if bb_width > 0 else 0.0
            )
            rsi_extremity = float(
                (self.RSI_OVERSOLD - rsi_14) / self.RSI_OVERSOLD
            )
            # 기본 0.70 + 보너스 최대 0.25 (각 요인 0.125)
            confidence = min(
                0.70 + (bb_penetration + rsi_extremity) * 0.125, 0.95
            )
            # [Phase 6] 변동성 돌파 보너스 +0.10 (현재 캔들 근사 방식, 최대 0.95 캡 적용)
            _vb_bonus = 0.0
            if _check_volatility_breakout():
                _vb_bonus = 0.10
                logger.info(
                    "🔥 [RuleBasedPredictor] 변동성 돌파 보너스 적용! BUY 신뢰도 %.4f → %.4f",
                    confidence, min(confidence + _vb_bonus, 0.95),
                )
            confidence = min(confidence + _vb_bonus, 0.95)
            logger.info(
                f"🟢 [RuleBasedPredictor] BUY 시그널 발생! "
                f"(Close: {close} <= BB_Lower: {bb_lower}) AND (RSI: {rsi_14} <= {self.RSI_OVERSOLD}) "
                f"| Confidence: {confidence:.4f}"
            )
            return "BUY", confidence

        # SELL: 현재 종가(close)가 볼린저 밴드 상단(bb_upper) 이상 '이고', RSI(rsi_14)가 70 이상일 때
        if close >= bb_upper and rsi_14 >= self.RSI_OVERBOUGHT:
            bb_penetration = (
                float((close - bb_upper) / bb_width) if bb_width > 0 else 0.0
            )
            rsi_extremity = float(
                (rsi_14 - self.RSI_OVERBOUGHT)
                / (Decimal("100") - self.RSI_OVERBOUGHT)
            )
            confidence = min(
                0.70 + (bb_penetration + rsi_extremity) * 0.125, 0.95
            )
            logger.info(
                f"🔴 [RuleBasedPredictor] SELL 시그널 발생! "
                f"(Close: {close} >= BB_Upper: {bb_upper}) AND (RSI: {rsi_14} >= {self.RSI_OVERBOUGHT}) "
                f"| Confidence: {confidence:.4f}"
            )
            return "SELL", confidence

        # HOLD: 그 외의 모든 상황
        logger.debug(
            f"⏸️  [RuleBasedPredictor] HOLD 유지: "
            f"Close={close}, RSI={rsi_14}, BB=[{bb_lower}, {bb_upper}]"
        )
        return "HOLD", 0.0



# ═══════════════════════════════════════════════════════════════════════════
# ML 기반 예측기 (Phase 4 실전 추론 엔진)
# ═══════════════════════════════════════════════════════════════════════════


class MLPredictor(BasePredictor):
    """
    LightGBM 모델 기반 실전 매매 시그널 추론 엔진.

    Phase 4 핵심 아키텍처:
      [1] model.pkl 싱글턴 로드 (부팅 시 1회 — joblib → pickle 체인)
      [2] DB에서 200봉 OHLCV 조회 → pandas DataFrame 구축
      [3] indicators.compute_all_features() → 12종 다중 피처 인메모리 벡터 연산
      [4] get_feature_vector() → 2D numpy array (1, 14) 변환
      [5] model.predict() + model.predict_proba() → 클래스 예측 + 확률
      [6] 신뢰도 가드라인: confidence < 65% → HOLD 자동 필터링
      [7] 상식 검증 가드: RSI/Stochastic 모순 → HOLD 전환
      [8] 어떤 예외든 → RuleBasedPredictor 자동 폴백 방패 가동

    Failure Modes (전부 HOLD 안전 착지):
      - model.pkl 파일 미존재 / 깨짐 → 폴백
      - DB 데이터 부족 (< 60봉) → 폴백
      - 피처 NaN (warm-up 미완료) → 폴백
      - 모델 추론 예외 (피처 불일치 등) → 폴백
      - 상식 검증 실패 (RSI 모순) → HOLD
    """

    # ── 상식 검증 가드 임계치 ─────────────────────────────────────────────
    _RSI_OVERBOUGHT_GUARD = 70.0    # BUY 시 RSI 과매수 차단선
    _RSI_OVERSOLD_GUARD = 30.0      # SELL 시 RSI 과매도 차단선
    _STOCH_OVERBOUGHT_GUARD = 80.0  # BUY 시 Stochastic %K 과매수 차단선
    _STOCH_OVERSOLD_GUARD = 20.0    # SELL 시 Stochastic %K 과매도 차단선

    # ── 데이터 요구사항 ───────────────────────────────────────────────────
    _QUERY_LIMIT = 200    # DB 조회 캔들 수
    _MIN_CANDLES = 60     # 최소 필요 캔들 수 (EMA 50 warm-up 보장)

    def __init__(
        self,
        session_factory=None,
        model_path: Optional[str] = None,
        confidence_threshold: float = 0.65,
    ):
        """
        MLPredictor 초기화 — 싱글턴 모델 로드 및 폴백 방패 준비.

        Args:
            session_factory: SQLAlchemy sessionmaker 인스턴스 (DB 조회용)
            model_path: model.pkl 파일 경로 (None → worker/model.pkl 자동 탐색)
            confidence_threshold: 신뢰도 가드라인 임계치 (기본 0.65)
        """
        self._session_factory = session_factory
        self._confidence_threshold = confidence_threshold
        self._model = None
        self._model_classes = None
        self._fallback = RuleBasedPredictor()

        # model.pkl 경로 결정 (미지정 시 현재 모듈 디렉토리 기준)
        if model_path is None:
            model_path = os.path.join(os.path.dirname(__file__), "model.pkl")

        self._load_model(model_path)

    def _load_model(self, path: str) -> None:
        """
        model.pkl을 joblib → pickle 체인으로 안전 로드 (부팅 시 1회 싱글턴).

        로드 실패 시 self._model = None 유지 → predict 호출 시 폴백 자동 가동.
        """
        if not os.path.isfile(path):
            logger.error(
                "❌ [MLPredictor] model.pkl 파일 미존재: %s → RuleBasedPredictor 폴백 대기",
                path,
            )
            return

        # 1차 시도: joblib (scikit-learn 호환 표준 로더)
        try:
            import joblib

            self._model = joblib.load(path)
            logger.info(
                "✅ [MLPredictor] model.pkl 로드 성공 (joblib): %s (%.1f MB)",
                path,
                os.path.getsize(path) / (1024 * 1024),
            )
        except Exception as exc_joblib:
            logger.warning(
                "⚠️ [MLPredictor] joblib 로드 실패 → pickle 2차 시도: %s",
                exc_joblib,
            )
            # 2차 시도: 네이티브 pickle
            try:
                with open(path, "rb") as f:
                    self._model = pickle.load(f)
                logger.info(
                    "✅ [MLPredictor] model.pkl 로드 성공 (pickle): %s", path
                )
            except Exception as exc_pickle:
                logger.error(
                    "❌ [MLPredictor] model.pkl 로드 최종 실패 → RuleBasedPredictor 폴백 가동\n"
                    "   joblib 오류: %s\n   pickle 오류: %s",
                    exc_joblib,
                    exc_pickle,
                )
                self._model = None
                return

        # 모델 클래스 레이블 추출 및 로깅
        if hasattr(self._model, "classes_"):
            self._model_classes = list(self._model.classes_)
            logger.info(
                "📋 [MLPredictor] 모델 클래스 레이블: %s", self._model_classes
            )

        # 모델 피처 개수 검증 (가능한 경우)
        if hasattr(self._model, "n_features_in_"):
            logger.info(
                "📋 [MLPredictor] 모델 기대 피처 수: %d | 파이프라인 피처 수: %d",
                self._model.n_features_in_,
                len(self._get_feature_columns()),
            )

    def _get_feature_columns(self) -> list:
        """
        모델이 학습 시 사용한 피처 컬럼 목록을 반환합니다.
        모델에 feature_name_ 속성이 있으면 그것을 사용하고,
        없으면 indicators.FEATURE_COLUMNS 표준 순서를 사용합니다.
        """
        if hasattr(self._model, "feature_name_"):
            return list(self._model.feature_name_)
        if hasattr(self._model, "feature_names_in_"):
            return list(self._model.feature_names_in_)

        from worker.indicators import FEATURE_COLUMNS

        return FEATURE_COLUMNS

    def _map_prediction_to_signal(
        self, prediction, probabilities: np.ndarray
    ) -> Tuple[str, float]:
        """
        모델 출력을 표준 시그널(BUY/SELL/HOLD)과 신뢰도로 매핑합니다.

        다양한 클래스 레이블 형식을 방어적으로 처리:
          - 정수: [0, 1, 2] → HOLD, BUY, SELL
          - 문자열: ["HOLD", "BUY", "SELL"] 또는 ["hold", "buy", "sell"]
        """
        classes = self._model_classes or list(range(len(probabilities)))

        # 최대 확률 클래스 추출
        max_idx = int(np.argmax(probabilities))
        max_confidence = float(probabilities[max_idx])
        predicted_class = classes[max_idx]

        # 클래스 → 시그널 매핑 (다양한 형식 방어적 대응)
        signal_str = str(predicted_class).upper().strip()
        if signal_str in ("BUY", "1", "LONG"):
            signal = "BUY"
        elif signal_str in ("SELL", "2", "SHORT"):
            signal = "SELL"
        else:
            signal = "HOLD"

        return signal, max_confidence

    def _sanity_guard(
        self, signal: str, confidence: float, features_row: pd.Series
    ) -> Tuple[str, float]:
        """
        상식 검증 가드: 기술적 지표와 모순되는 시그널을 차단합니다.

        [검증 규칙]
        1. RSI ≥ 70 (과매수) 상태에서 BUY 시그널 → HOLD (모순 — 고점 추격 매수 차단)
        2. RSI ≤ 30 (과매도) 상태에서 SELL 시그널 → HOLD (모순 — 바닥 투매 차단)
        3. Stochastic %K ≥ 80 (과매수) 상태에서 BUY → HOLD (이중 검증)
        4. Stochastic %K ≤ 20 (과매도) 상태에서 SELL → HOLD (이중 검증)

        검증 통과 시: 원본 시그널·신뢰도 반환
        검증 실패 시: HOLD 전환, 신뢰도 × 0.3 페널티 적용
        """
        rsi = float(features_row.get("rsi_14", 50.0))
        stoch_k = float(features_row.get("stoch_k", 50.0))

        if signal == "BUY":
            if rsi >= self._RSI_OVERBOUGHT_GUARD:
                logger.warning(
                    "🛡️ [상식 검증 가드] RSI=%.1f 과매수 상태에서 BUY 시그널 감지 "
                    "→ HOLD 전환 (고점 추격 뇌동매매 차단)",
                    rsi,
                )
                return "HOLD", confidence * 0.3
            if stoch_k >= self._STOCH_OVERBOUGHT_GUARD:
                logger.warning(
                    "🛡️ [상식 검증 가드] Stochastic %%K=%.1f 과매수 구간에서 BUY 시그널 감지 "
                    "→ HOLD 전환",
                    stoch_k,
                )
                return "HOLD", confidence * 0.3

        elif signal == "SELL":
            if rsi <= self._RSI_OVERSOLD_GUARD:
                logger.warning(
                    "🛡️ [상식 검증 가드] RSI=%.1f 과매도 상태에서 SELL 시그널 감지 "
                    "→ HOLD 전환 (바닥 투매 뇌동매매 차단)",
                    rsi,
                )
                return "HOLD", confidence * 0.3
            if stoch_k <= self._STOCH_OVERSOLD_GUARD:
                logger.warning(
                    "🛡️ [상식 검증 가드] Stochastic %%K=%.1f 과매도 구간에서 SELL 시그널 감지 "
                    "→ HOLD 전환",
                    stoch_k,
                )
                return "HOLD", confidence * 0.3

        return signal, confidence

    def predict_with_confidence(self, market_data: MarketData) -> Tuple[str, float]:
        """
        ML 모델 기반 매매 시그널 + 신뢰도 추론 파이프라인.

        실행 흐름:
          [1] 모델 로드 상태 확인 → 미로드 시 폴백
          [2] DB에서 200봉 OHLCV 조회
          [3] 인메모리 다중 피처 벡터 연산
          [4] NaN 검증 (warm-up 미완료 방어)
          [5] model.predict() + predict_proba() 추론
          [6] 신뢰도 가드라인 (< 65% → HOLD)
          [7] 상식 검증 가드 (RSI·Stochastic 모순 감지)
          [8] 예외 발생 시 RuleBasedPredictor 폴백

        Args:
            market_data: 최신 MarketData ORM 인스턴스

        Returns:
            Tuple[str, float]: ("BUY"/"SELL"/"HOLD", 0.0~1.0 신뢰도)
        """
        # ── STEP 1: 모델 미로드 시 즉시 폴백 ────────────────────────────
        if self._model is None:
            logger.info(
                "⚙️ [MLPredictor] 모델 미로드 상태 → RuleBasedPredictor 폴백 가동"
            )
            return self._fallback.predict_with_confidence(market_data)

        try:
            # ── STEP 2: 심볼 추출 ───────────────────────────────────────
            if isinstance(market_data, dict):
                symbol = market_data.get("symbol", "BTC/USDT")
            else:
                symbol = getattr(market_data, "symbol", "BTC/USDT")

            # ── STEP 3: DB에서 200봉 조회 ───────────────────────────────
            if self._session_factory is None:
                raise RuntimeError(
                    "DB 세션 팩토리(session_factory)가 주입되지 않았습니다."
                )

            session = self._session_factory()
            try:
                from sqlalchemy import desc

                rows = (
                    session.query(MarketData)
                    .filter(MarketData.symbol == symbol)
                    .order_by(desc(MarketData.timestamp))
                    .limit(self._QUERY_LIMIT)
                    .all()
                )
            finally:
                session.close()

            # ── STEP 4: 최소 캔들 수 검증 ───────────────────────────────
            if len(rows) < self._MIN_CANDLES:
                logger.info(
                    "⏸️ [MLPredictor] 데이터 부족: %d봉 < 최소 %d봉 "
                    "→ RuleBasedPredictor 폴백",
                    len(rows),
                    self._MIN_CANDLES,
                )
                return self._fallback.predict_with_confidence(market_data)

            # ── STEP 5: DataFrame 구축 (시간순 정렬: oldest → newest) ───
            rows.reverse()
            df = pd.DataFrame(
                [
                    {
                        # ── [Timezone Fix] timestamp를 UTC-aware로 정규화 ────────────────
                        # DB DateTime(timezone=True) 콼럼은 timezone-aware를 반환하지만,
                        # 혼재 naive datetime이 있을 경우를 대비하여 UTC millisecond로 정규화
                        "timestamp_ms": int(
                            (
                                r.timestamp.astimezone(timezone.utc)
                                if r.timestamp.tzinfo is not None
                                else r.timestamp.replace(tzinfo=timezone.utc)
                            ).timestamp() * 1000
                        ),
                        "open":   float(r.open),
                        "high":   float(r.high),
                        "low":    float(r.low),
                        "close":  float(r.close),
                        "volume": float(r.volume),
                    }
                    for r in rows
                ]
            )
            # ── [Timezone Fix] timestamp_ms 기준 오름차순 정렬 적용 ────────────────
            # UTC/KST 혼재로 인한 9시간 간격 데이터 공백(타임스탬프 순서 역전)을
            # 방지하여 compute_all_features()가 연속 시계열로 재료를 받도록 보장
            df = df.sort_values("timestamp_ms", ascending=True).reset_index(drop=True)

            # ── STEP 6: 다중 피처 벡터 연산 (인메모리) ──────────────────
            from worker.indicators import (
                compute_all_features,
                get_feature_vector,
                has_valid_features,
                FEATURE_COLUMNS,
            )

            df = compute_all_features(df)

            # ── STEP 7: 마지막 행 피처 추출 및 NaN 검증 ─────────────────
            last_row = df.iloc[-1]
            if not has_valid_features(last_row):
                logger.warning(
                    "⚠️ [MLPredictor] 피처 벡터에 NaN 감지 (warm-up 미완료) "
                    "→ RuleBasedPredictor 폴백"
                )
                return self._fallback.predict_with_confidence(market_data)

            # 모델이 기대하는 피처 순서로 벡터 조립
            model_feature_cols = self._get_feature_columns()
            try:
                features = last_row[model_feature_cols].values.astype(np.float64)
            except KeyError:
                # 모델이 기대하는 피처와 파이프라인 피처 불일치 → 표준 순서로 폴백
                logger.warning(
                    "⚠️ [MLPredictor] 모델 피처 키 불일치 → 표준 FEATURE_COLUMNS 사용"
                )
                features = get_feature_vector(last_row)

            X = features.reshape(1, -1)

            # ── STEP 8: 모델 추론 ───────────────────────────────────────
            prediction = self._model.predict(X)[0]

            if hasattr(self._model, "predict_proba"):
                probabilities = self._model.predict_proba(X)[0]
            else:
                # predict_proba 미지원 모델 대응 (확률 하드코딩)
                n_classes = len(
                    self._model_classes
                    if self._model_classes
                    else [0, 1, 2]
                )
                probabilities = np.zeros(n_classes)
                pred_idx = (
                    int(prediction)
                    if isinstance(prediction, (int, float, np.integer))
                    else 0
                )
                if 0 <= pred_idx < n_classes:
                    probabilities[pred_idx] = 1.0

            signal, confidence = self._map_prediction_to_signal(
                prediction, probabilities
            )

            logger.info(
                "🤖 [MLPredictor] 모델 추론 완료 — signal=%s, confidence=%.4f, "
                "probabilities=%s, 피처수=%d, 캔들수=%d",
                signal,
                confidence,
                [f"{p:.4f}" for p in probabilities],
                len(features),
                len(rows),
            )

            # ── STEP 9: [Phase 6] 동적 확신도 임계치 산출 ───────────────
            # 기본값: self._confidence_threshold (0.65)
            # 완화 조건(어느 하나라도 충족 시 → 0.55 하향):
            #   [A] 현재 거래량 > 직전 20봉 평균 거래량 (Volume Surge)
            #   [B] RSI(14) 40~60 구간 (강한 추세 진행 중)
            # Decimal 정밀도 준수 및 NaN 방어 내장.
            _effective_threshold = self._confidence_threshold  # 기본 0.65
            try:
                _rsi_val = float(last_row.get("rsi_14", 50.0))
                _vol_val = float(last_row.get("volume", 0.0))

                # [A] 거래량 급등 판정: 직전 20봉 평균 거래량과 비교
                _vol_surge = False
                if len(df) >= 21 and not math.isnan(_vol_val) and _vol_val > 0:
                    _recent_vols = [
                        float(v) for v in df["volume"].iloc[-21:-1].values
                        if not math.isnan(float(v))
                    ]
                    if _recent_vols:
                        _avg_vol = sum(_recent_vols) / len(_recent_vols)
                        if _avg_vol > 0 and _vol_val > _avg_vol:
                            _vol_surge = True
                            logger.info(
                                "⚡ [DYN_THRESHOLD] 거래량 급등 감지 — "
                                "현재 Vol=%.2f > 20봉 평균=%.2f → 임계치 0.65→0.55",
                                _vol_val, _avg_vol,
                            )

                # [B] RSI 40~60 강한 추세 구간 판정
                _rsi_trend_zone = (
                    not math.isnan(_rsi_val)
                    and 40.0 <= _rsi_val <= 60.0
                )
                if _rsi_trend_zone and not _vol_surge:
                    logger.info(
                        "⚡ [DYN_THRESHOLD] RSI 추세 구간 감지 — "
                        "RSI=%.1f (40~60) → 임계치 0.65→0.55",
                        _rsi_val,
                    )

                if _vol_surge or _rsi_trend_zone:
                    _effective_threshold = 0.55

            except Exception as _thr_exc:
                # 임계치 산출 실패 시 기본값(0.65) 유지 — 기존 안전 로직 보존
                logger.warning(
                    "⚠️ [DYN_THRESHOLD] 동적 임계치 산출 실패 → 기본값 %.2f 유지: %s",
                    self._confidence_threshold, _thr_exc,
                )

            # ── STEP 9-B: [Phase 6] 변동성 돌파 보너스 +0.20 (래리 윌리엄스) ──
            # ML 추론 결과가 BUY이고, 래리 윌리엄스 변동성 돌파 조건 충족 시
            # 신뢰도에 +0.20 무조건 추가 (최대 0.99 캡, sanity guard 이전 단계).
            _vb_bonus = 0.0
            if signal == "BUY":
                try:
                    _open_val   = float(last_row.get("open",   float("nan")))
                    _high_val   = float(last_row.get("high",   float("nan")))
                    _low_val    = float(last_row.get("low",    float("nan")))
                    _close_val  = float(last_row.get("close",  float("nan")))
                    _vol_vb     = float(last_row.get("volume", 0.0))
                    if not any(math.isnan(v) for v in (_open_val, _high_val, _low_val, _close_val)):
                        _day_range    = _high_val - _low_val
                        _target_price = _open_val + _day_range * 0.5
                        if _day_range > 0 and _close_val > _target_price and _vol_vb > 0:
                            _vb_bonus = 0.20
                            logger.info(
                                "🔥 [VB_BREAKOUT/ML] 변동성 돌파 보너스 적용! "
                                "BUY 신뢰도 %.4f → %.4f "
                                "(Close=%.2f > Target=%.2f)",
                                confidence, min(confidence + _vb_bonus, 0.99),
                                _close_val, _target_price,
                            )
                except Exception as _vb_exc:
                    logger.warning(
                        "⚠️ [VB_BREAKOUT/ML] 변동성 돌파 판정 중 예외 → 보너스 미적용: %s",
                        _vb_exc,
                    )
            confidence = min(confidence + _vb_bonus, 0.99)

            # ── STEP 9-C: 유효 임계치 기반 신뢰도 가드 ─────────────────
            if signal != "HOLD" and confidence < _effective_threshold:
                logger.info(
                    "🛡️ [MLPredictor] 신뢰도 가드: %.2f%% < %.2f%% (유효 임계치) "
                    "→ HOLD 필터링 (뇌동매매 방지)",
                    confidence * 100,
                    _effective_threshold * 100,
                )
                return "HOLD", confidence

            # ── STEP 10: 상식 검증 가드 (RSI·Stochastic 모순 감지) ──────
            signal, confidence = self._sanity_guard(
                signal, confidence, last_row
            )

            return signal, confidence

        except Exception as exc:
            logger.error(
                "❌ [MLPredictor] 추론 파이프라인 예외 → RuleBasedPredictor 폴백 가동: %s",
                exc,
                exc_info=True,
            )
            return self._fallback.predict_with_confidence(market_data)

    def predict(self, market_data: MarketData) -> str:
        """하위 호환 인터페이스: 매매 시그널 문자열만 반환합니다."""
        action, _ = self.predict_with_confidence(market_data)
        return action


# ═══════════════════════════════════════════════════════════════════════════
# 추세추종 예측기 (4h EMA 교차 — 백테스트로 검증된 양방향 엣지)
# ═══════════════════════════════════════════════════════════════════════════


class TrendFollowingPredictor(BasePredictor):
    """
    상위 타임프레임(기본 4h) EMA 교차 기반 추세추종 예측기.

    ── 도입 근거 (scripts/signal_research.py · validate_trend.py 검증) ──────────
    1m 평균회귀(RuleBased/ML)는 180일 실데이터에서 엣지 0(무수수료 PF 1.00)으로
    수수료에 잠식되어 -16.5% 손실. 반면 4h EMA 교차는 롱·숏 양방향 흑자 +
    월 5/7 흑자 + 파라미터 전 구간 양수(과최적 아님) + 슬리피지 무관(저빈도)으로
    슬리피지 포함 +57~60%, Sharpe ~2.4, MDD -17%를 기록.

    신호 규칙 (always-in, stop-and-reverse):
      최근 완성된 4h봉에서 EMA(fast) > EMA(slow) → "BUY" (상승추세 → 롱)
                            EMA(fast) < EMA(slow) → "SELL"(하락추세 → 숏)
    엔진은 이 신호로 자연스럽게 추세전환 시 스톱앤리버스를 수행한다.

    ⚠️ 주의: 이 예측기를 켜면(PREDICTOR_TYPE=TREND) tasks.py가 평균회귀용 가드
    (4h 타임아웃·+3% 하드TP·-1% 타이트SL)를 자동 비활성화한다. 그 가드들은
    추세추종의 '승자를 길게 태우는' 엣지를 파괴하기 때문이다.
    """

    def __init__(
        self,
        session_factory=None,
        ema_fast: int = 30,
        ema_slow: int = 60,
        timeframe_minutes: int = 240,  # 4h
    ):
        self._session_factory = session_factory
        self.ema_fast = ema_fast
        self.ema_slow = ema_slow
        self.tf_min = timeframe_minutes
        # 상위 TF봉을 (ema_slow + 버퍼)개 확보하기 위한 1분봉 조회량
        self._query_limit = int(timeframe_minutes * (ema_slow + 20) * 1.2)
        self._min_htf_bars = ema_slow + 2
        self._fallback = RuleBasedPredictor()
        logger.info(
            "📈 [TrendFollowingPredictor] 초기화 — EMA(%d/%d) on %dm, 1m조회=%d봉",
            ema_fast, ema_slow, timeframe_minutes, self._query_limit,
        )

    @staticmethod
    def compute_signal(df_1m: pd.DataFrame, ema_fast: int, ema_slow: int,
                       tf_min: int) -> Tuple[str, float]:
        """
        1분봉 DataFrame(컬럼: timestamp_ms, open/high/low/close/volume, 시간오름차순)을
        상위 TF로 리샘플 후 EMA 교차 신호를 반환. (DB 독립 — 단위검증 가능)
        """
        if df_1m is None or len(df_1m) == 0:
            return "HOLD", 0.0
        d = df_1m.copy()
        d["dt"] = pd.to_datetime(d["timestamp_ms"], unit="ms", utc=True)
        htf = (
            d.set_index("dt")
            .resample(f"{tf_min}min")
            .agg({"open": "first", "high": "max", "low": "min",
                  "close": "last", "volume": "sum"})
            .dropna()
        )
        if len(htf) < ema_slow + 2:
            return "HOLD", 0.0
        ef = htf["close"].ewm(span=ema_fast, adjust=False).mean()
        es = htf["close"].ewm(span=ema_slow, adjust=False).mean()
        # 마지막 '완성' 봉 기준 (resample의 마지막 봉은 미완성일 수 있어 직전 봉 사용)
        f_last, s_last = float(ef.iloc[-2]), float(es.iloc[-2])
        if np.isnan(f_last) or np.isnan(s_last):
            return "HOLD", 0.0
        sep = abs(f_last - s_last) / s_last if s_last else 0.0
        conf = float(min(0.5 + sep * 20, 0.95))  # 이격이 클수록 확신↑ (정보용)
        return ("BUY" if f_last > s_last else "SELL"), conf

    def predict_with_confidence(self, market_data: MarketData) -> Tuple[str, float]:
        if self._session_factory is None:
            logger.warning("⚠️ [TrendFollowing] session_factory 미주입 → RuleBased 폴백")
            return self._fallback.predict_with_confidence(market_data)
        try:
            if isinstance(market_data, dict):
                symbol = market_data.get("symbol", "BTC/USDT")
            else:
                symbol = getattr(market_data, "symbol", "BTC/USDT")

            from sqlalchemy import desc
            session = self._session_factory()
            try:
                rows = (
                    session.query(MarketData)
                    .filter(MarketData.symbol == symbol)
                    .order_by(desc(MarketData.timestamp))
                    .limit(self._query_limit)
                    .all()
                )
            finally:
                session.close()

            if not rows:
                logger.info("⏸️ [TrendFollowing] 캔들 없음 → HOLD (워밍업 대기)")
                return "HOLD", 0.0

            rows.reverse()  # oldest → newest
            df = pd.DataFrame([
                {
                    "timestamp_ms": int(
                        (r.timestamp.astimezone(timezone.utc)
                         if r.timestamp.tzinfo else
                         r.timestamp.replace(tzinfo=timezone.utc)).timestamp() * 1000
                    ),
                    "open": float(r.open), "high": float(r.high),
                    "low": float(r.low), "close": float(r.close),
                    "volume": float(r.volume),
                }
                for r in rows
            ]).sort_values("timestamp_ms").reset_index(drop=True)

            sig, conf = self.compute_signal(df, self.ema_fast, self.ema_slow, self.tf_min)
            logger.info(
                "📈 [TrendFollowing] %s 신호=%s (conf=%.2f, EMA%d/%d on %dm)",
                symbol, sig, conf, self.ema_fast, self.ema_slow, self.tf_min,
            )
            return sig, conf
        except Exception as exc:
            logger.error(
                "❌ [TrendFollowing] 추론 예외 → RuleBased 폴백: %s", exc, exc_info=True
            )
            return self._fallback.predict_with_confidence(market_data)

    def predict(self, market_data: MarketData) -> str:
        action, _ = self.predict_with_confidence(market_data)
        return action
