import os
from dotenv import load_dotenv

load_dotenv()

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "include/events.db")
SQLITE_TABLE_NAME = os.getenv("SQLITE_TABLE_NAME", "event_data")

GIT_REPO_PATH = os.getenv("GIT_REPO_PATH", "Dev-Events")
MARKDOWN_FILE_PATH = os.getenv("MARKDOWN_FILE_PATH", "Dev-Events/README.md")
GITHUB_USERNAME = os.getenv("GITHUB_USERNAME")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

REPO_URL = "https://github.com/SUSC-KR/Dev-Events.git"
REPO_PATH = "/tmp/Dev-Events"
MARKDOWN_FILE_PATH = os.path.join(REPO_PATH, "README.md")
