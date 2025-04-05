from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from pendulum import datetime, duration
import pandas as pd
import logging
import sqlite3

from include.custom_platform_functions import EventUsCrawler, MeetupsCrawler
from include.utils import execute_sql, delete_past_events
from include.config import SQLITE_DB_PATH, SQLITE_TABLE_NAME

t_log = logging.getLogger("airflow.task")

EVENT_CRAWLERS = [EventUsCrawler, MeetupsCrawler]

EVENTS_UPDATED_DATASET = Dataset(SQLITE_DB_PATH)


@dag(
    start_date=datetime(2025, 3, 1),
    schedule="@daily",
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    default_args={
        "owner": "SUSC",
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["event-us", "meetups", "ETL", "crawler"],
)
def dev_event_crawler():

    @task
    def create_event_table():
        execute_sql(
            SQLITE_DB_PATH,
            f"""
            CREATE TABLE IF NOT EXISTS {SQLITE_TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                url TEXT UNIQUE,
                date TEXT,
                description TEXT,
                price TEXT,
                location TEXT,
                host TEXT
            )
        """,
        )
        t_log.info("이벤트 테이블 생성 완료.")

    @task
    def delete_old_events():
        delete_past_events(SQLITE_DB_PATH, SQLITE_TABLE_NAME)
        t_log.info("지난 이벤트 삭제 완료.")

    @task
    def crawl_events() -> pd.DataFrame:
        all_events = [
            crawler().fetch_events_upcoming_2_months()
            for crawler in EVENT_CRAWLERS
        ]
        return (
            pd.concat(
                [df for df in all_events if not df.empty], ignore_index=True
            )
            if all_events
            else pd.DataFrame()
        )

    @task(outlets=[EVENTS_UPDATED_DATASET])
    def insert_events(events: pd.DataFrame):
        if events.empty:
            t_log.info("삽입할 이벤트 없음.")
            return

        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()

        for _, event in events.iterrows():
            cursor.execute(
                f"""
                INSERT INTO {SQLITE_TABLE_NAME} (title, url, date, description, price, location, host)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    title = excluded.title, date = excluded.date,
                    description = excluded.description, price = excluded.price,
                    location = excluded.location, host = excluded.host
            """,
                (
                    event["title"],
                    event["url"],
                    event["date"],
                    event["description"],
                    event["price"],
                    event["location"],
                    event["host"],
                ),
            )

        conn.commit()
        conn.close()
        t_log.info("이벤트 데이터 삽입 완료.")

    create_event_table() >> delete_old_events() >> insert_events(crawl_events())


dev_event_crawler()
