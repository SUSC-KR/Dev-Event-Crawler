from airflow.decorators import (
    dag,
    task,
)
from airflow.models.dataset import Dataset
from airflow.models.baseoperator import chain
from pendulum import datetime, duration
import pandas as pd
import sqlite3
import logging
import os
import pendulum

from include.custom_platform_functions import EventUsCrawler, MeetupsCrawler

t_log = logging.getLogger("airflow.task")

_SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "include/events.db")
_SQLITE_TABLE_NAME = os.getenv("SQLITE_TABLE_NAME", "event_data")

EVENT_CRAWLERS = [
    EventUsCrawler,
    MeetupsCrawler,
    # 추후 새로운 크롤러 추가 가능
]


@dag(
    start_date=datetime(2025, 3, 1),
    schedule="@daily",
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "SUSC",
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["event-us", "meetups", "ETL", "crawler"],
)
def dev_event_crawler():

    @task(retries=2)
    def create_event_table_in_sqlite(
        db_path: str = _SQLITE_DB_PATH, table_name: str = _SQLITE_TABLE_NAME
    ) -> None:
        t_log.info("SQLite에 이벤트 테이블을 생성합니다.")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                url TEXT,
                date TEXT,
                description TEXT,
                price TEXT,
                location TEXT,
                host TEXT,
                UNIQUE(url)
            )
            """
        )
        conn.commit()
        conn.close()
        t_log.info("SQLite에 이벤트 테이블이 생성되었습니다.")

    @task
    def crawl_events() -> pd.DataFrame:
        """등록된 모든 크롤러에서 이벤트 데이터를 가져옴"""
        all_events = []
        for Crawler in EVENT_CRAWLERS:
            crawler = Crawler()
            t_log.info(f"Crawling events from {Crawler.__name__}")
            events = crawler.fetch_events_upcoming_2_months()
            if events is not None and not events.empty:
                all_events.append(events)

        if all_events:
            merged_events = pd.concat(all_events, ignore_index=True)
        else:
            merged_events = pd.DataFrame(
                columns=[
                    "title",
                    "url",
                    "date",
                    "description",
                    "price",
                    "location",
                    "host",
                ]
            )

        t_log.info(
            f"Total {len(merged_events)} events crawled from all sources."
        )
        return merged_events

    @task
    def delete_past_events_from_sqlite(
        db_path: str = _SQLITE_DB_PATH,
        table_name: str = _SQLITE_TABLE_NAME,
    ) -> None:
        t_log.info("SQLite에서 지난 이벤트를 삭제합니다.")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        today = pendulum.today().to_date_string()

        cursor.execute(
            f"""
            DELETE FROM {table_name} WHERE date < ?
            """,
            (today,),
        )
        conn.commit()
        conn.close()
        t_log.info("SQLite에서 지난 이벤트를 삭제했습니다.")

    @task(outlets=[Dataset(_SQLITE_DB_PATH)])
    def insert_or_update_events_into_sqlite(
        events: pd.DataFrame,
        db_path: str = _SQLITE_DB_PATH,
        table_name: str = _SQLITE_TABLE_NAME,
    ) -> None:
        t_log.info("SQLite에 이벤트를 삽입하거나 업데이트합니다.")
        if events.empty:
            t_log.info("삽입하거나 업데이트할 이벤트가 없습니다.")
            return

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for _, event in events.iterrows():
            cursor.execute(
                f"""
                INSERT INTO {table_name} (title, url, date, description, price, location, host)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url)
                DO UPDATE SET
                    title = excluded.title,
                    date = excluded.date,
                    description = excluded.description,
                    price = excluded.price,
                    location = excluded.location,
                    host = excluded.host
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
        t_log.info("SQLite에 이벤트가 삽입되거나 업데이트되었습니다.")

    create_event_table_in_sqlite_obj = create_event_table_in_sqlite()
    crawl_events_obj = crawl_events()
    delete_past_events_obj = delete_past_events_from_sqlite()

    insert_or_update_events_into_sqlite_obj = (
        insert_or_update_events_into_sqlite(crawl_events_obj)
    )

    chain(
        create_event_table_in_sqlite_obj,
        delete_past_events_obj,
        insert_or_update_events_into_sqlite_obj,
    )


dev_event_crawler()
