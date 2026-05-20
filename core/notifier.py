import logging
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import httpx
from core.config import get_settings

logger = logging.getLogger(__name__)

class TradeNotifier:
    """
    QuantFlow 실시간 거래 상태 전송용 텔레그램 알림 시스템.
    정밀한 포맷팅과 완벽한 예외 복원력(Resilience)을 제공하여 매매 루프의 안정성을 보장합니다.
    """
    def __init__(self):
        self.settings = get_settings()
        # 1. Pydantic Settings에서 읽어오고, 없을 시 직접 os.getenv 폴백 지원
        self.bot_token = getattr(self.settings, 'telegram_bot_token', '') or os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.chat_id = getattr(self.settings, 'telegram_chat_id', '') or os.getenv('TELEGRAM_CHAT_ID', '')
        self.is_enabled = bool(self.bot_token and self.chat_id)

        if not self.is_enabled:
            logger.warning("⚠️ Telegram Notifier가 비활성화 상태입니다. (.env 내 TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID를 확인하세요)")
        else:
            logger.info("🟢 Telegram Notifier가 성공적으로 활성화되었습니다.")

    def send_message(self, message: str) -> bool:
        """
        텔레그램 API를 이용해 포맷된 HTML 메시지를 채널로 동기 발송합니다.
        (Celery 워커 스레드 내에서 안전하게 동기로 수행되도록 설계되었습니다.)
        """
        if not self.is_enabled:
            logger.debug("Telegram notification skipped: bot token or chat id not configured.")
            return False

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        
        try:
            with httpx.Client() as client:
                response = client.post(url, json=payload, timeout=5.0)
                if response.status_code == 200:
                    logger.info("✅ 텔레그램 알림 전송 완료.")
                    return True
                else:
                    logger.warning(f"⚠️ 텔레그램 API 전송 실패: HTTP {response.status_code} - {response.text}")
                    return False
        except Exception as e:
            logger.error(f"❌ 텔레그램 통신 중 예외 발생: {e}")
            return False

    def notify_trade(
        self,
        trigger_type: str,
        symbol: str,
        side: str,
        price,
        amount,
        order_id: str,
        confidence,
        balance
    ) -> bool:
        """
        매매 시그널 및 주문 체결 결과를 프리미엄 HTML 서식으로 포맷팅하여 텔레그램 채널로 전송합니다.
        네트워크 장애나 데이터 타입 에러 등이 발생하더라도 매매 엔진 전체가 크래시되지 않도록 완벽히 보호합니다.
        """
        try:
            if not self.is_enabled:
                logger.debug("Telegram TradeNotifier is disabled (Missing token or chat_id).")
                return False

            # 1. 안전한 수치 타입 변환 (Decimal, float, str 모두 방어적으로 처리)
            try:
                dec_price = Decimal(str(price)) if price is not None else Decimal("0")
                dec_amount = Decimal(str(amount)) if amount is not None else Decimal("0")
                dec_balance = Decimal(str(balance)) if balance is not None else Decimal("0")
                total_usdt = dec_price * dec_amount
            except Exception as parse_err:
                logger.warning(f"⚠️ notify_trade 내 수치 파싱 실패: {parse_err}")
                dec_price = price
                dec_amount = amount
                dec_balance = balance
                total_usdt = None

            # 2. 트리거 타입에 따른 프리미엄 이모지 & 타이틀 매핑
            trigger_emoji = "🔔"
            title_text = f"[{trigger_type}] 신호 알림"
            
            upper_trigger = str(trigger_type).upper()
            if "STOP_LOSS_SHIELD" in upper_trigger:
                trigger_emoji = "🚨"
                title_text = "STOP_LOSS_SHIELD 손절 방패 작동"
            elif "SNIPER_ENTRY" in upper_trigger:
                trigger_emoji = "🎯"
                title_text = "SNIPER_ENTRY 스나이퍼 매매 진입"
            elif "REVERSE_SWITCH_EXIT" in upper_trigger:
                trigger_emoji = "🔄"
                title_text = "REVERSE_SWITCH_EXIT 역시그널 청산"

            # 3. 매매 방향(Side) 이모지 및 가독성 텍스트 매핑
            upper_side = str(side).upper()
            if upper_side == "BUY":
                side_display = "🟢 BUY (매수)"
            elif upper_side == "SELL":
                side_display = "🔴 SELL (매도/청산)"
            else:
                side_display = f"⚪ {side}"

            # 4. 소수점 및 화폐 천 단위 구분 쉼표 포맷팅 헬퍼
            def format_currency(val, is_crypto=False):
                if val is None:
                    return "N/A"
                if isinstance(val, (int, float, Decimal)):
                    if is_crypto:
                        return f"{val:,.4f}"
                    else:
                        return f"{val:,.2f}"
                return str(val)

            price_str = format_currency(dec_price)
            amount_str = format_currency(dec_amount, is_crypto=True)
            total_usdt_str = format_currency(total_usdt)
            balance_str = format_currency(dec_balance)

            # 5. 진입 확신도(confidence) 포맷팅
            confidence_str = "N/A"
            if confidence is not None:
                try:
                    conf_float = float(confidence)
                    # 0.0 ~ 1.0 사이 비율값인 경우 퍼센트로 변환, 1.0 초과면 백분율이 이미 곱해진 것으로 간주
                    if 0.0 <= conf_float <= 1.0:
                        confidence_str = f"{conf_float * 100:.1f}%"
                    else:
                        confidence_str = f"{conf_float:.1f}%"
                except Exception:
                    confidence_str = str(confidence)

            # 6. 발생 시각 (한국 시간 KST 고정 표기하여 높은 직관성 확보)
            kst_tz = timezone(timedelta(hours=9))
            now_kst = datetime.now(kst_tz).strftime("%Y-%m-%d %H:%M:%S KST")

            # 7. 프리미엄 HTML 서식 메시지 템플릿 생성
            message_lines = [
                f"{trigger_emoji} <b>{title_text}</b>",
                "━━━━━━━━━━━━━━━━━━━━",
                f"• <b>심볼(Asset):</b> <code>{symbol}</code>",
                f"• <b>주문 방향:</b> {side_display}",
                f"• <b>체결 단가:</b> <code>${price_str}</code>",
                f"• <b>주문 수량:</b> <code>{amount_str} BTC</code>",
                f"• <b>체결 총액:</b> <code>${total_usdt_str} USDT</code>",
                f"• <b>가용 잔고:</b> <code>${balance_str} USDT</code>",
                "────────────────────",
                f"• <b>진입 확신도:</b> <code>{confidence_str}</code>",
                f"• <b>주문 번호:</b> <code>{order_id}</code>",
                f"• <b>알림 시각:</b> <code>{now_kst}</code>",
                "━━━━━━━━━━━━━━━━━━━━"
            ]
            
            message = "\n".join(message_lines)
            
            # 8. 메시지 전송
            return self.send_message(message)

        except Exception as exc:
            # 🛡️ 강력한 예외 가드: 네트워크 장애/포맷팅 장애로 메인 매매 프로세스가 멈추지 않도록 무조건 에러 로그만 남김
            logger.error(f"❌ TradeNotifier.notify_trade 내부에서 치명적인 예외가 감지되었으나 안전하게 격리되었습니다: {exc}", exc_info=True)
            return False

# [하위 호환성 유지] tasks.py 및 외부 모듈에서 'notifier' 인스턴스로 즉시 사용할 수 있도록 인스턴스 바인딩 및 익스포트
notifier = TradeNotifier()

def send_telegram_message(message: str) -> bool:
    """
    외부에서 임의의 커스텀 메시지를 발송할 수 있도록 지원하는 전역 헬퍼 함수입니다.
    """
    return notifier.send_message(message)
