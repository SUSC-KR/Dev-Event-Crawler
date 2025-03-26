from airflow.decorators import dag, task
from pendulum import datetime, duration
from dotenv import load_dotenv
import sqlite3
import os
import logging
import subprocess

load_dotenv()
t_log = logging.getLogger("airflow.task")

_SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "include/events.db")
_SQLITE_TABLE_NAME = os.getenv("SQLITE_TABLE_NAME", "event_data")
_MARKDOWN_FILE_PATH = os.getenv("MARKDOWN_FILE_PATH", "Dev-Events/README.md")
_GIT_REPO_PATH = os.getenv("GIT_REPO_PATH", "Dev-Events")
_GITHUB_USERNAME = os.environ.get("GITHUB_USERNAME")
_GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")


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
    tags=["Github", "README"],
)
def modify_and_commit():

    @task(retries=2)
    def modify_readme(
        db_path: str = _SQLITE_DB_PATH,
        table_name: str = _SQLITE_TABLE_NAME,
        output_file: str = _MARKDOWN_FILE_PATH,
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
            markdown_output += "현재 개발자 행사를 모아두는 다양한 플랫폼이 존재하지만, 대부분의 정보는 수동 등록이 필요하며 지속적인 관리가 필요합니다."
            markdown_output += "이 프로젝트는 행사 주최자와 프로젝트 관리자의 이러한 반복적인 작업을 자동화하여 최신 개발자 행사 정보를 손쉽게 제공하는 것을 목표로 합니다.\n\n"
            markdown_output += "새로운 행사 사이트 추가 또는 버그 제보는 "
            markdown_output += "[프로젝트 레포지토리](https://github.com/SUSC-KR/Dev-Event-Crawler)의 "
            markdown_output += "이슈 페이지를 통해 해주시면 감사하겠습니다.\n\n"
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
    def commit_changes(file_path: str) -> None:
        commit_message = "Updated README.md via Airflow DAG"

        try:
            subprocess.run(
                ["git", "config", "user.name", "Yeonguk"],
                cwd=_GIT_REPO_PATH,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.email", "choo121600@gmail.com"],
                cwd=_GIT_REPO_PATH,
                check=True,
            )
            status_result = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=_GIT_REPO_PATH,
                capture_output=True,
                text=True,
                check=True,
            )

            if not status_result.stdout.strip():
                t_log.info("No changes to commit.")
                return

            subprocess.run(
                ["git", "add", "."],
                cwd=_GIT_REPO_PATH,
                check=True,
                capture_output=True,
                text=True,
            )

            commit_result = subprocess.run(
                ["git", "commit", "-m", commit_message],
                cwd=_GIT_REPO_PATH,
                check=True,
                capture_output=True,
                text=True,
            )
            if commit_result.returncode != 0:
                t_log.warning("No changes were committed.")

            git_url = f"https://{_GITHUB_USERNAME}:{_GITHUB_TOKEN}@github.com/SUSC-KR/Dev-Events.git"
            push_result = subprocess.run(
                ["git", "push", git_url, "main"],
                cwd=_GIT_REPO_PATH,
                check=True,
                capture_output=True,
                text=True,
            )
            if push_result.returncode == 0:
                t_log.info("Changes committed and pushed successfully.")
            else:
                t_log.warning("Failed to push changes.")

        except subprocess.CalledProcessError as e:
            t_log.error(f"Git command failed: {e.stderr}")
            raise
        except Exception as e:
            t_log.error(f"Unexpected error during Git commit: {e}")
            raise

    file_path = modify_readme()
    commit_changes(file_path)


modify_and_commit_dag = modify_and_commit()
