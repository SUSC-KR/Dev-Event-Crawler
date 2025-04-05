import os
import sqlite3
import subprocess
import logging
import pendulum

t_log = logging.getLogger("airflow.task")


def execute_sql(db_path, query, params=()):
    """SQLite에 SQL 실행 함수"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()
    conn.close()


def delete_past_events(db_path, table_name):
    """이전 이벤트 삭제"""
    today = pendulum.today().to_date_string()
    execute_sql(
        db_path,
        f"DELETE FROM {table_name} WHERE date < ?",
        (today,),
    )
    t_log.info("지난 이벤트가 삭제되었습니다.")


def commit_and_push_changes(repo_path, commit_message):
    """Git 커밋 및 푸시"""
    try:
        subprocess.run(
            ["git", "config", "user.name", os.getenv("GITHUB_USERNAME")],
            cwd=repo_path,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.email", os.getenv("GITHUB_EMAIL")],
            cwd=repo_path,
            check=True,
        )

        status_result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo_path,
            capture_output=True,
            text=True,
        )

        if not status_result.stdout.strip():
            t_log.info("No changes to commit.")
            return

        subprocess.run(["git", "add", "."], cwd=repo_path, check=True)
        subprocess.run(
            ["git", "commit", "-m", commit_message], cwd=repo_path, check=True
        )

        git_url = f"https://{os.getenv('GITHUB_USERNAME')}:{os.getenv('GITHUB_TOKEN')}@github.com/SUSC-KR/Dev-Events.git"
        subprocess.run(
            ["git", "push", git_url, "main"], cwd=repo_path, check=True
        )

        t_log.info("Changes committed and pushed successfully.")

    except subprocess.CalledProcessError as e:
        t_log.error(f"Git command failed: {e.stderr}")
        raise
