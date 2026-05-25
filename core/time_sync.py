"""
core.time_sync — 거래소 서버와의 시간 차이 검증
타임스탬프 기반 API 서명 오류를 사전 방지
"""

import logging
import time

import ntplib

logger = logging.getLogger(__name__)

# 허용 가능한 최대 시간 차이 (밀리초)
MAX_DRIFT_MS = 1000


def check_ntp_drift() -> float:
    """
    NTP 서버와 로컬 시계의 차이(ms)를 반환.
    3회 재시도 및 다중 NTP 서버 폴백 지원으로 복원력 극대화.
    """
    ntp_servers = ["pool.ntp.org", "time.google.com", "time.windows.com"]
    client = ntplib.NTPClient()
    
    for attempt in range(1, 4):
        for server in ntp_servers:
            try:
                response = client.request(server, version=3, timeout=2.0)
                drift_ms = response.offset * 1000  # 초 → 밀리초

                if abs(drift_ms) > MAX_DRIFT_MS:
                    logger.warning(
                        f"⚠ 시간 drift 감지: {drift_ms:.1f}ms ({server}) (허용: ±{MAX_DRIFT_MS}ms). "
                        f"거래소 API 오류 가능성 있음!"
                    )
                else:
                    logger.info(f"✅ NTP 시간 동기화 정상: drift={drift_ms:.1f}ms ({server})")
                return drift_ms
            except Exception as e:
                logger.debug(f"NTP 서버 {server} (attempt {attempt}/3) 조회 실패: {e}")
        time.sleep(1.0)
    
    logger.error("❌ 모든 NTP 서버 조회 실패 (3회 시도 완료)")
    return 0.0


def get_timestamp_ms() -> int:
    """현재 시각을 밀리초 Unix 타임스탬프로 반환"""
    return int(time.time() * 1000)
