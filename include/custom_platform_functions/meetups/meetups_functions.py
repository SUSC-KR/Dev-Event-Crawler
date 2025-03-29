import time
import pandas as pd
import logging
import datetime
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from ...format_functions.format_event_date import format_event_date

from .settings import (
    SEARCH_URL,
    WAIT_TIME,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class MeetupsCrawler:
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

    def remove_url_params(self, url: str) -> str:
        parsed_url = urlparse(url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

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
        self.driver.implicitly_wait(WAIT_TIME)

        event_title = self.get_element_text(['//*[@id="main"]/div[2]/div/h1'])

        event_date = self.get_element_text(
            [
                '//*[@id="event-info"]/div[1]/div[1]/div[1]/div[2]/div/time',
                '//*[@id="event-info"]/div[1]/div[1]/div[2]/div[2]/div/time',
            ]
        )
        event_description = self.get_element_text(['//*[@id="event-details"]'])
        event_price = self.get_element_text(
            ['//*[@id="main"]/div[4]/div/div/div[2]/div/div[1]/div/div/span']
        )
        event_location = self.get_element_text(
            [
                '//*[@id="event-info"]/div[1]/div[1]/div[3]/div/div[2]/div',
                '//*[@id="event-info"]/div[1]/div[1]/div[4]/div/div[2]/div',
            ]
        )
        event_host = self.get_element_text(
            [
                '//*[@id="main"]/div[3]/div[1]/div/div[2]/div[2]/div[1]/div/a/div/div[1]/div'
            ]
        )

        if not event_description:
            logging.warning(f"Description missing for event: {event_url}")
            return None

        event_date = format_event_date(event_date)

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
        """이벤트 목록을 크롤링하고 세부 정보 추출 (스크롤 방식)"""
        start_date, end_date = self.get_date_range_upcoming_2_months()
        date_range_url = f"S&customStartDate={start_date}T11%3A00%3A00-04%3A00&customEndDate={end_date}T10%3A59%3A00-04%3A00"
        mile_range_url = "&distance=hundredMiles&location=kr--Seoul"
        scrap_url = SEARCH_URL + date_range_url + mile_range_url
        logging.info(f"크롤링할 URL: {scrap_url}")
        self.driver.get(scrap_url)

        event_urls = []

        self.driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);"
        )
        time.sleep(2)

        parent_element = self.driver.find_element(
            By.XPATH, '//*[@id="main"]/div/div[2]/div[1]/div/div/div[1]'
        )

        child_divs = parent_element.find_elements(By.XPATH, "./div")
        logging.info(f"현재 페이지의 이벤트 개수: {len(child_divs)}")

        for i in range(1, len(child_divs)):
            event_xpath = f'//*[@id = "main"]/div/div[2]/div[1]/div/div/div/div[{i}]/div/div/div[1]/div[2]/a'
            event_url = self.driver.find_element(
                By.XPATH, event_xpath
            ).get_attribute("href")
            if event_url:
                event_url = self.remove_url_params(event_url)
                event_urls.append(event_url)

            if len(event_urls) == 0:
                logging.info("No more events to scrape.")

        for event_url in event_urls:
            logging.info(f"크롤링 중: {event_url}")
            event_data = self.scrape_event_details(event_url)
            if event_data:
                self.events_data.append(event_data)

    def fetch_events_upcoming_2_months(self) -> pd.DataFrame:
        """
        Meetups 웹사이트에서 향후 2개월 동안 열릴 행사를 가져옴.
        """
        self.scrape_events()
        event_df = pd.DataFrame(self.events_data)
        self.close()
        return event_df

    def close(self):
        """브라우저 종료"""
        self.driver.quit()
