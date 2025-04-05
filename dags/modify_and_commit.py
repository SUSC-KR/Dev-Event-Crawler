from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from pendulum import datetime, duration
import sqlite3
import logging
import os

from include.utils import commit_and_push_changes
from include.config import SQLITE_DB_PATH, SQLITE_TABLE_NAME, MARKDOWN_FILE_PATH

t_log = logging.getLogger("airflow.task")


@dag(
    start_date=datetime(2025, 3, 1),
    schedule=[Dataset(SQLITE_DB_PATH)],
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    default_args={
        "owner": "SUSC",
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["Github", "README"],
)
def modify_and_commit():

    @task(retries=2)
    def modify_readme(
        db_path: str = SQLITE_DB_PATH,
        table_name: str = SQLITE_TABLE_NAME,
        output_file: str = MARKDOWN_FILE_PATH,
    ) -> str:
        t_log.info(f"Connecting to SQLite database: {db_path}")

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            query = f"""
            SELECT title, url, date, price, host, location
            FROM {table_name}
            ORDER BY date ASC;
            """
            cursor.execute(query)
            events = cursor.fetchall()

            if not events:
                t_log.warning("No events found in the database.")
                return output_file

            markdown_output = "# Event List\n\n"
            markdown_output += "## 프로젝트의 목적\n\n"
            markdown_output += (
                "현재 개발자 행사를 모아두는 다양한 플랫폼이 존재하지만, "
                "대부분의 정보는 수동 등록이 필요하며 지속적인 관리가 필요합니다. "
                "이 프로젝트는 행사 주최자와 프로젝트 관리자의 이러한 반복적인 작업을 자동화하여 "
                "최신 개발자 행사 정보를 손쉽게 제공하는 것을 목표로 합니다.\n\n"
            )
            markdown_output += (
                "새로운 행사 사이트 추가 또는 버그 제보는 "
                "[프로젝트 레포지토리](https://github.com/SUSC-KR/Dev-Event-Crawler)의 "
                "이슈 페이지를 통해 해주시면 감사하겠습니다.\n\n"
            )
            markdown_output += "## 이벤트 목록\n\n"
            for event in events:
                title, url, date, price, host, location = event
                markdown_output += f"### [{title}]({url})\n"
                markdown_output += f"- **Date:** {date}\n"
                markdown_output += f"- **Price:** {price}\n"
                markdown_output += f"- **Host:** {host}\n"
                markdown_output += f"- **Location:** {location}\n\n"

            with open(output_file, "w", encoding="utf-8") as md_file:
                md_file.write(markdown_output)

            t_log.info(f"README file updated: {output_file}")

        except Exception as e:
            t_log.error(f"Error updating README: {e}")
            raise
        finally:
            conn.close()

        return output_file

    @task
    def commit_changes(file_path: str):
        commit_message = "Updated README.md via Airflow DAG"
        repo_path = os.path.dirname(file_path)
        commit_and_push_changes(repo_path, commit_message)

    commit_changes(modify_readme())


modify_and_commit_dag = modify_and_commit()
