import time
import pandas as pd
import logging
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from include.custom_platform_functions.eventus.settings import (
    SEARCH_URL,
    WAIT_TIME,
    NEXT_BUTTON_XPATH,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class EventUsCrawler:
    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument(
            "--disable-dev-shm-usage"
        )  # /dev/shm 사용 안 함
        # chrome_options.add_argument("--disable-gpu")  # GPU 가속 비활성화
        # chrome_options.add_argument("--remote-debugging-port=9222")  # 디버깅 활성화

        remote_webdriver = "selenium-chromium"
        self.driver = webdriver.Remote(
            command_executor=f"http://{remote_webdriver}:4444/wd/hub",
            # service=Service(ChromeDriverManager().install()),
            options=chrome_options,
        )
        self.driver.implicitly_wait(WAIT_TIME)
        self.events_data = []

    def get_element_text(self, xpaths: list):
        """리스트에 있는 모든 XPath에서 텍스트를 순차적으로 찾고, 텍스트가 있으면 반환"""
        for xpath in xpaths:
            try:
                element = self.driver.find_element(By.XPATH, xpath)
                if element:
                    return element.text.strip()
            except Exception:
                continue

        return None

    def scrape_event_details(self, event_url):
        """개별 행사 페이지에서 세부 정보를 크롤링"""
        self.driver.get(event_url)

        event_title = self.get_element_text(
            [
                '//*[@id="app"]/div[1]/div[2]/div[1]/div[2]/div/div/div[2]',
                '//*[@id="app"]/div[1]/div[2]/div[1]/div[1]/div/div/div[2]',
                '//*[@id="app"]/div[1]/div[3]/div[1]/div[2]/div/div/div[2]',
            ]
        )

        event_date = self.get_element_text(
            ['//*[@id="infoSection"]/dl/div[1]/dd/span']
        )
        event_description = self.get_element_text(
            ['//*[@id="infoSection"]/div[2]/div[1]']
        )
        event_price = self.get_element_text(
            ['//*[@id="infoSection"]/dl/div[3]/dd/span']
        )
        event_location = self.get_element_text(
            ['//*[@id="infoSection"]/dl/div[4]/dd/span']
        )
        event_host = self.get_element_text(
            ['//*[@id="channelSection"]/div[2]/div[1]/div/div/div[1]/a/span']
        )

        if not event_description:
            logging.warning(f"Description missing for event: {event_url}")
            return None

        return {
            "title": event_title,
            "url": event_url,
            "date": event_date,
            "description": event_description,
            "price": event_price,
            "location": event_location,
            "host": event_host,
        }

    def get_date_range_upcoming_2_months(self):
        """오늘부터 2달 뒤까지의 날짜 범위 계산"""
        today = datetime.date.today()
        two_months_later = today + datetime.timedelta(days=60)
        return today.strftime("%Y-%m-%d"), two_months_later.strftime("%Y-%m-%d")

    def scrape_events(self):
        """이벤트 목록을 크롤링하고 세부 정보 추출"""
        start_date, end_date = self.get_date_range_upcoming_2_months()
        date_range_url = f"&date={start_date}~{end_date}"
        self.driver.get(SEARCH_URL + date_range_url)

        event_urls = []

        while True:
            for i in range(1, 13):  # 한 페이지당 최대 12개 이벤트
                try:
                    event_xpath = f'//*[@id="eventSection"]/div[3]/div[{i}]/div/div[2]/div[2]/a'
                    event_url = self.driver.find_element(
                        By.XPATH, event_xpath
                    ).get_attribute("href")
                    if event_url:
                        event_urls.append(event_url)
                except Exception as e:
                    logging.error(f"Error fetching event link {i}: {e}")
                    continue

            if not self.click_next():
                break

        for event_url in event_urls:
            logging.info(f"크롤링 중: {event_url}")
            event_data = self.scrape_event_details(event_url)
            if event_data:
                self.events_data.append(event_data)

    def click_next(self):
        """다음 페이지 버튼 클릭"""
        try:
            next_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, NEXT_BUTTON_XPATH))
            )
            next_button.click()
            time.sleep(WAIT_TIME)
            logging.info("다음 페이지 이동")
            return True
        except Exception:
            logging.info("다음 페이지가 없습니다.")
            return False

    def fetch_events_upcoming_2_months(self) -> pd.DataFrame:
        """
        Eventus 웹사이트에서 향후 2개월 동안 열릴 행사를 가져옴.
        """
        self.scrape_events()
        eventus_event_df = pd.DataFrame(self.events_data)
        self.close()
        return eventus_event_df

    def close(self):
        """브라우저 종료"""
        self.driver.quit()
