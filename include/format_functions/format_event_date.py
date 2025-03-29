import re
from datetime import datetime


def format_event_date(date_str: str):
    """
    주어진 날짜 문자열을 'YYYY-MM-DD HH:MM:SS' 형식으로 변환.
    """

    # Type 1, Type 2: 한국어 형식
    if "월" in date_str and "일" in date_str and " ~ " in date_str:
        start_str, end_str = date_str.split(" ~ ")

        # Type 1: 시작과 종료 모두 '월', '일'이 포함된 경우
        if "월" in end_str and "일" in end_str:
            try:
                # 날짜와 시간 추출
                start_match = re.search(
                    r"(\d{1,2})월 (\d{1,2})일.*?(\d{1,2}):(\d{1,2})", start_str
                )
                end_match = re.search(
                    r"(\d{1,2})월 (\d{1,2})일.*?(\d{1,2}):(\d{1,2})", end_str
                )

                if not start_match or not end_match:
                    raise ValueError("날짜 형식 오류")

                start_month, start_day, start_hour, start_minute = (
                    start_match.groups()
                )
                end_month, end_day, end_hour, end_minute = end_match.groups()

                start_date = f"2025-{start_month.zfill(2)}-{start_day.zfill(2)} {start_hour.zfill(2)}:{start_minute.zfill(2)}:00"
                end_date = f"2025-{end_month.zfill(2)}-{end_day.zfill(2)} {end_hour.zfill(2)}:{end_minute.zfill(2)}:00"

                return f"{start_date} ~ {end_date}"
            except Exception as e:
                print(f"Type 1 처리 오류: {e}")
                return "Error"

        # Type 2: 시작 날짜에는 '월', '일'이 있지만 종료 시간에는 없는 경우
        else:
            try:
                # 날짜와 시간 추출
                start_match = re.search(
                    r"(\d{1,2})월 (\d{1,2})일.*?(\d{1,2}):(\d{1,2})", start_str
                )

                if not start_match:
                    raise ValueError("날짜 형식 오류")

                start_month, start_day, start_hour, start_minute = (
                    start_match.groups()
                )
                end_hour, end_minute = end_str.split(":")

                start_date = f"2025-{start_month.zfill(2)}-{start_day.zfill(2)} {start_hour.zfill(2)}:{start_minute.zfill(2)}:00"
                end_date = f"2025-{start_month.zfill(2)}-{start_day.zfill(2)} {end_hour.zfill(2)}:{end_minute.zfill(2)}:00"

                return f"{start_date} ~ {end_date}"
            except Exception as e:
                print(f"Type 2 처리 오류: {e}")
                return "Error"

    # Type 3: 영어 형식, AM/PM 포함
    elif "to" in date_str:
        try:
            start_str, end_str = date_str.split(" to ")

            # KST 또는 UTC 시간대 정보를 제거
            start_str = (
                start_str.strip().replace(" KST", "").replace(" UTC", "")
            )
            end_str = end_str.strip().replace(" KST", "").replace(" UTC", "")

            # 시작 날짜와 시간 변환
            start_datetime = datetime.strptime(
                start_str, "%A, %B %d, %Y %I:%M %p"
            )
            start_date = start_datetime.strftime("%Y-%m-%d %H:%M:%S")

            # 종료 시간 변환 같은 날로 간주
            end_datetime = datetime.strptime(end_str, "%I:%M %p")
            end_date = f"{start_datetime.strftime('%Y-%m-%d')} {end_datetime.strftime('%H:%M:%S')}"

            return f"{start_date} ~ {end_date}"
        except Exception as e:
            print(f"Type 3 처리 오류: {e}")
            return "Error"

    return "Invalid format"
