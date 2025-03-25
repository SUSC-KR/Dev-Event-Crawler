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

from include.custom_platform_functions.eventus.eventus_functions import (
    EventUsCrawler,
)

t_log = logging.getLogger("airflow.task")

_SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "include/events.db")
_SQLITE_TABLE_NAME = os.getenv("SQLITE_TABLE_NAME", "event_data")


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
    tags=["event-us", "ETL", "crawler"],
)
def dev_event_crawler():

    @task(retries=2)
    def create_event_table_in_sqlite(
        db_path: str = _SQLITE_DB_PATH, table_name: str = _SQLITE_TABLE_NAME
    ) -> None:
        t_log.info("Creating event table in SQLite.")
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
        t_log.info("Event table created in SQLite.")

    @task
    def crawl_events() -> pd.DataFrame:
        t_log.info("Crawling events.")
        crawler = EventUsCrawler()
        events = crawler.fetch_events_upcoming_2_months()
        t_log.info("Events crawled.")
        return events

    @task
    def delete_past_events_from_sqlite(
        db_path: str = _SQLITE_DB_PATH,
        table_name: str = _SQLITE_TABLE_NAME,
    ) -> None:
        t_log.info("Deleting past events from SQLite.")
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
        t_log.info("Past events deleted from SQLite.")

    @task(outlets=[Dataset(_SQLITE_DB_PATH)])
    def insert_or_update_events_into_sqlite(
        events: pd.DataFrame,
        db_path: str = _SQLITE_DB_PATH,
        table_name: str = _SQLITE_TABLE_NAME,
    ) -> None:
        t_log.info("Inserting or updating events into SQLite.")
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
        t_log.info("Events inserted or updated in SQLite.")

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
