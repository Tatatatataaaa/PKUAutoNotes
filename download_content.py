from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from getpass import getpass
from pathlib import Path
from typing import Any

from pku_auto_notes.downloader import (
    DEFAULT_APP_ID,
    DEFAULT_HISTORY_DB_NAME,
    DEFAULT_REDIR_URL,
    PKUCourseDownloader,
)

CONFIG_FILE_PATH = Path(__file__).resolve().parent / "pku_autonotes_config.json"


@dataclass
class RunConfig:
    username: str
    password: str
    appid: str
    redir_url: str
    mode: str
    page_url: str | None
    file_path: Path
    log_file: Path | None
    auto_add_suffix: bool
    overwrite: bool
    list_only: bool
    max_files: int | None
    max_courses: int | None
    max_pages_per_course: int
    course_keyword: str | None
    use_custom_sso: bool
    remember_password: bool


def _prompt_text(prompt: str, default: str | None = None, required: bool = False) -> str:
    while True:
        suffix = f" [{default}]" if default else ""
        value = input(f"{prompt}{suffix}: ").strip()
        if value:
            return value
        if default is not None:
            return default
        if not required:
            return ""
        print("This field is required.")


def _prompt_bool(prompt: str, default: bool = False) -> bool:
    default_text = "Y/n" if default else "y/N"
    while True:
        value = input(f"{prompt} [{default_text}]: ").strip().lower()
        if not value:
            return default
        if value in {"y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False
        print("Please enter y or n.")


def _prompt_optional_int(prompt: str, default: int | None = None) -> int | None:
    default_text = str(default) if default is not None else "blank for no limit"
    while True:
        raw = input(f"{prompt} [{default_text}]: ").strip()
        if not raw:
            return default
        if raw.isdigit():
            return int(raw)
        print("Please enter a non-negative integer, or leave blank.")


def _prompt_positive_int(prompt: str, default: int) -> int:
    while True:
        raw = input(f"{prompt} [{default}]: ").strip()
        if not raw:
            return default
        if raw.isdigit() and int(raw) > 0:
            return int(raw)
        print("Please enter a positive integer.")


def _prompt_password() -> str:
    attempts = 3
    for _ in range(attempts):
        try:
            password = getpass("IAAA password (hidden input): ")
        except (EOFError, KeyboardInterrupt) as exc:
            raise RuntimeError("password input cancelled") from exc

        if password:
            return password

        print("Password cannot be empty. Please try again.")

    raise RuntimeError("password cannot be empty after 3 attempts")


def _resolve_password_interactive(saved: dict[str, Any]) -> tuple[str, bool]:
    remember_default = _saved_bool(saved, "remember_password", False)
    saved_password = _saved_str(saved, "password", None) if remember_default else None

    env_password = os.getenv("PKU_PASSWORD")
    if env_password and _prompt_bool("Use password from PKU_PASSWORD", default=False):
        remember_password = _prompt_bool(
            "Remember password in local config file (plain text)",
            default=remember_default,
        )
        return env_password, remember_password

    if saved_password and _prompt_bool("Use password saved in local config", default=True):
        return saved_password, remember_default

    password = _prompt_password()
    remember_password = _prompt_bool(
        "Remember password in local config file (plain text)",
        default=remember_default,
    )
    return password, remember_password


def _prompt_mode(default: str = "all") -> str:
    default_choice = "1" if default == "single" else "2"
    print("\nSelect run mode:")
    print("1) Single content page")
    print("2) All courses")
    while True:
        choice = input(f"Enter 1 or 2 [{default_choice}]: ").strip() or default_choice
        if choice == "1":
            return "single"
        if choice == "2":
            return "all"
        print("Please enter 1 or 2.")


def _saved_str(saved: dict[str, Any], key: str, default: str | None = None) -> str | None:
    value = saved.get(key, default)
    return value if isinstance(value, str) else default


def _saved_bool(saved: dict[str, Any], key: str, default: bool = False) -> bool:
    value = saved.get(key, default)
    return value if isinstance(value, bool) else default


def _saved_optional_int(saved: dict[str, Any], key: str, default: int | None = None) -> int | None:
    value = saved.get(key, default)
    if value is None:
        return None
    return value if isinstance(value, int) and value >= 0 else default


def _load_json_config() -> dict[str, Any]:
    if not CONFIG_FILE_PATH.exists():
        return {}

    try:
        raw = json.loads(CONFIG_FILE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Warning: failed to read config file {CONFIG_FILE_PATH.name}: {exc}")
        return {}

    return raw if isinstance(raw, dict) else {}


def _save_json_config(config: RunConfig) -> None:
    data: dict[str, Any] = {
        "username": config.username,
        "appid": config.appid,
        "redir_url": config.redir_url,
        "mode": config.mode,
        "page_url": config.page_url,
        "file_path": str(config.file_path),
        "output_dir": str(config.file_path),
        "log_file": str(config.log_file) if config.log_file else None,
        "auto_add_suffix": config.auto_add_suffix,
        "overwrite": config.overwrite,
        "list_only": config.list_only,
        "max_files": config.max_files,
        "max_courses": config.max_courses,
        "max_pages_per_course": config.max_pages_per_course,
        "course_keyword": config.course_keyword,
        "use_custom_sso": config.use_custom_sso,
        "remember_password": config.remember_password,
    }

    if config.remember_password and config.password:
        data["password"] = config.password

    CONFIG_FILE_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _setup_logging(log_file: Path | None) -> logging.Logger:
    logger = logging.getLogger("pkuautonotes")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        try:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except OSError as exc:
            print(f"Warning: failed to initialize log file {log_file}: {exc}")

    return logger


def _build_default_run_config(saved: dict[str, Any]) -> RunConfig:
    username = os.getenv("PKU_USERNAME") or _saved_str(saved, "username", "") or ""
    remember_password = _saved_bool(saved, "remember_password", False)
    saved_password = _saved_str(saved, "password", "") if remember_password else ""
    env_password = os.getenv("PKU_PASSWORD") or ""
    password = env_password or (saved_password or "")

    use_custom_sso = _saved_bool(saved, "use_custom_sso", False)
    appid_default = _saved_str(saved, "appid", DEFAULT_APP_ID) or DEFAULT_APP_ID
    redir_default = _saved_str(saved, "redir_url", DEFAULT_REDIR_URL) or DEFAULT_REDIR_URL

    mode = _saved_str(saved, "mode", "all") or "all"
    if mode not in {"single", "all"}:
        mode = "all"

    output_raw = _saved_str(saved, "file_path", None) or _saved_str(saved, "output_dir", "downloads") or "downloads"
    log_file_raw = saved.get("log_file", "logs/pku_autonotes.log")
    log_file: Path | None
    if isinstance(log_file_raw, str):
        log_file = Path(log_file_raw) if log_file_raw.strip() else None
    elif log_file_raw is None:
        log_file = None
    else:
        log_file = Path("logs/pku_autonotes.log")
    page_url = _saved_str(saved, "page_url", None)
    max_pages = _saved_optional_int(saved, "max_pages_per_course", 120) or 120
    if max_pages < 1:
        max_pages = 120

    keyword = _saved_str(saved, "course_keyword", None)
    keyword = keyword if keyword else None

    return RunConfig(
        username=username,
        password=password,
        appid=appid_default if use_custom_sso else DEFAULT_APP_ID,
        redir_url=redir_default if use_custom_sso else DEFAULT_REDIR_URL,
        mode=mode,
        page_url=page_url,
        file_path=Path(output_raw),
        log_file=log_file,
        auto_add_suffix=_saved_bool(saved, "auto_add_suffix", True),
        overwrite=_saved_bool(saved, "overwrite", False),
        list_only=_saved_bool(saved, "list_only", False),
        max_files=_saved_optional_int(saved, "max_files"),
        max_courses=_saved_optional_int(saved, "max_courses"),
        max_pages_per_course=max_pages,
        course_keyword=keyword,
        use_custom_sso=use_custom_sso,
        remember_password=remember_password,
    )


def _password_source(config: RunConfig) -> str:
    env_password = os.getenv("PKU_PASSWORD")
    if env_password and config.password == env_password:
        return "PKU_PASSWORD"
    if config.password and config.remember_password:
        return "saved-config"
    if config.password:
        return "session"
    return "missing"


def _render_optional(value: Any) -> str:
    if value is None:
        return "None"
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value).strip()
    return text if text else "None"


def _print_config_summary(config: RunConfig) -> None:
    print("\nCurrent configuration:")
    print(f"- username: {_render_optional(config.username)}")
    print(f"- password_source: {_password_source(config)}")
    print(f"- remember_password: {_render_optional(config.remember_password)}")
    print(f"- use_custom_sso: {_render_optional(config.use_custom_sso)}")
    print(f"- appid: {_render_optional(config.appid)}")
    print(f"- redir_url: {_render_optional(config.redir_url)}")
    print(f"- mode: {_render_optional(config.mode)}")
    print(f"- page_url: {_render_optional(config.page_url)}")
    print(f"- file_path: {_render_optional(config.file_path)}")
    print(f"- log_file: {_render_optional(config.log_file)}")
    print(f"- auto_add_suffix: {_render_optional(config.auto_add_suffix)}")
    print(f"- list_only: {_render_optional(config.list_only)}")
    print(f"- overwrite: {_render_optional(config.overwrite)}")
    print(f"- max_files: {_render_optional(config.max_files)}")
    print(f"- max_courses: {_render_optional(config.max_courses)}")
    print(f"- max_pages_per_course: {_render_optional(config.max_pages_per_course)}")
    print(f"- course_keyword: {_render_optional(config.course_keyword)}")


def _edit_run_config_interactively(config: RunConfig, saved: dict[str, Any]) -> RunConfig:
    username = _prompt_text("IAAA username", default=config.username or None, required=True)

    password = config.password
    remember_password = config.remember_password
    if password:
        if not _prompt_bool("Keep current password setting", default=True):
            password, remember_password = _resolve_password_interactive(saved)
    else:
        password, remember_password = _resolve_password_interactive(saved)

    use_custom_sso = _prompt_bool("Use custom IAAA appid/redirUrl", default=config.use_custom_sso)
    appid = DEFAULT_APP_ID
    redir_url = DEFAULT_REDIR_URL
    if use_custom_sso:
        appid = _prompt_text("IAAA app id", default=config.appid, required=True)
        redir_url = _prompt_text("SSO redirect URL", default=config.redir_url, required=True)

    mode = _prompt_mode(default=config.mode)
    list_only = _prompt_bool("List only (no download)", default=config.list_only)
    file_path = Path(_prompt_text("File path (download root)", default=str(config.file_path), required=True))
    log_file_default = str(config.log_file) if config.log_file else ""
    log_file_raw = _prompt_text(
        "Log file path (blank disables file log)",
        default=log_file_default,
        required=False,
    )
    log_file = Path(log_file_raw) if log_file_raw else None
    auto_add_suffix = _prompt_bool("Auto append filename suffix", default=config.auto_add_suffix)
    overwrite = _prompt_bool("Overwrite files with same name", default=config.overwrite)
    max_files = _prompt_optional_int("Max files per content page", default=config.max_files)

    page_url: str | None = None
    max_courses: int | None = None
    max_pages_per_course = config.max_pages_per_course if config.max_pages_per_course > 0 else 120
    course_keyword: str | None = None

    if mode == "single":
        page_url = _prompt_text("Course content page URL", default=config.page_url, required=True)
    else:
        max_courses = _prompt_optional_int("Max courses to process", default=config.max_courses)
        max_pages_per_course = _prompt_positive_int("Max pages per course", default=max_pages_per_course)
        keyword_default = config.course_keyword or ""
        keyword = _prompt_text("Course keyword filter", default=keyword_default, required=False)
        course_keyword = keyword or None

    return RunConfig(
        username=username,
        password=password,
        appid=appid,
        redir_url=redir_url,
        mode=mode,
        page_url=page_url,
        file_path=file_path,
        log_file=log_file,
        auto_add_suffix=auto_add_suffix,
        overwrite=overwrite,
        list_only=list_only,
        max_files=max_files,
        max_courses=max_courses,
        max_pages_per_course=max_pages_per_course,
        course_keyword=course_keyword,
        use_custom_sso=use_custom_sso,
        remember_password=remember_password,
    )


def _ensure_runtime_required_fields(config: RunConfig, saved: dict[str, Any]) -> RunConfig:
    if not config.username:
        config.username = _prompt_text("IAAA username", default=None, required=True)

    if not config.password:
        config.password, config.remember_password = _resolve_password_interactive(saved)

    if config.mode == "single" and not config.page_url:
        config.page_url = _prompt_text("Course content page URL", default=None, required=True)

    if not str(config.file_path).strip():
        config.file_path = Path("downloads")

    if config.max_pages_per_course < 1:
        config.max_pages_per_course = 120

    return config


def collect_run_config() -> RunConfig:
    print("PKUAutoNotes interactive setup")
    print("Config is loaded from JSON and shown once for confirmation.\n")

    saved = _load_json_config()
    print(f"Config file: {CONFIG_FILE_PATH.name}")

    config = _build_default_run_config(saved)
    _print_config_summary(config)

    if not _prompt_bool("Confirm and continue with the above config", default=True):
        print("\nEditing configuration...")
        config = _edit_run_config_interactively(config, saved)

    return _ensure_runtime_required_fields(config, saved)


def main() -> int:
    try:
        config = collect_run_config()
    except RuntimeError as exc:
        print(f"Input error: {exc}")
        return 2

    try:
        _save_json_config(config)
    except OSError as exc:
        print(f"Warning: failed to save config file {CONFIG_FILE_PATH.name}: {exc}")

    logger = _setup_logging(config.log_file)
    logger.info(
        "Run started | mode=%s | list_only=%s | file_path=%s | auto_add_suffix=%s",
        config.mode,
        config.list_only,
        config.file_path,
        config.auto_add_suffix,
    )
    logger.info("Runtime | python=%s", sys.version.split()[0])
    if config.log_file:
        logger.info("File logging enabled: %s", config.log_file)

    history_db_path = config.file_path / DEFAULT_HISTORY_DB_NAME
    logger.info("Download history database: %s", history_db_path)

    client = PKUCourseDownloader(
        auto_add_suffix=config.auto_add_suffix,
        history_db_path=history_db_path,
    )
    logger.info("Logging in via IAAA...")
    try:
        client.login(
            username=config.username,
            password=config.password,
            appid=config.appid,
            redir_url=config.redir_url,
        )
    except RuntimeError as exc:
        logger.error("Login failed: %s", exc)
        logger.info("Tip: keep default appid/redirUrl unless you need a special setup.")
        return 1

    logger.info("Login successful.")

    if config.mode == "all":
        try:
            courses = client.discover_current_term_courses()
        except RuntimeError as exc:
            logger.error("Course discovery failed: %s", exc)
            if not _prompt_bool("Switch to single content page mode now", default=True):
                return 1

            config.mode = "single"
            config.page_url = _prompt_text("Course content page URL", required=True)
            try:
                _save_json_config(config)
            except OSError as save_exc:
                logger.warning("Failed to save config file %s: %s", CONFIG_FILE_PATH.name, save_exc)
            courses = []

        if config.mode == "all":
            if config.course_keyword:
                keyword = config.course_keyword.lower().strip()
                courses = [course for course in courses if keyword in course.title.lower()]

            if config.max_courses is not None:
                courses = courses[: max(config.max_courses, 0)]

            if config.list_only:
                logger.info("Found %s current-term courses:", len(courses))
                for idx, course in enumerate(courses, start=1):
                    logger.info("[%s] %s (%s)", idx, course.title, course.course_id)
                    logger.info("    %s", course.entry_url)
                return 0

            results = client.download_all_courses(
                output_root=config.file_path,
                overwrite=config.overwrite,
                max_courses=config.max_courses,
                max_pages_per_course=max(config.max_pages_per_course, 1),
                max_files_per_page=config.max_files,
                course_keyword=config.course_keyword,
                current_term_only=True,
                teaching_content_only=True,
            )

            total_downloaded = sum(len(item.downloaded) for item in results)
            total_skipped = sum(len(item.skipped) for item in results)
            logger.info("Processed courses: %s", len(results))
            logger.info("Total downloaded: %s", total_downloaded)
            logger.info("Total skipped: %s", total_skipped)

            for item in results:
                logger.info(
                    "- %s (%s) | pages=%s, downloaded=%s, skipped=%s",
                    item.course.title,
                    item.course.course_id,
                    item.pages_scanned,
                    len(item.downloaded),
                    len(item.skipped),
                )

            return 0

    assert config.page_url is not None
    coursewares = client.list_teaching_content_coursewares(config.page_url)

    if config.max_files is not None:
        coursewares = coursewares[: max(config.max_files, 0)]

    if config.list_only:
        if coursewares:
            logger.info("Found %s coursewares in teaching-content page:", len(coursewares))
            for idx, item in enumerate(coursewares, start=1):
                cid = item.content_id or "N/A"
                logger.info("[%s] %s (content_id=%s)", idx, item.title, cid)
                logger.info("    %s", item.file_url)
            return 0

        targets = client.list_download_targets(config.page_url)
        logger.info("Found %s downloadable links:", len(targets))
        for idx, (url, title) in enumerate(targets, start=1):
            logger.info("[%s] %s", idx, title)
            logger.info("    %s", url)
        return 0

    if coursewares:
        summary = client.download_from_teaching_content_page(
            page_url=config.page_url,
            output_dir=config.file_path,
            overwrite=config.overwrite,
            max_files=config.max_files,
        )
    else:
        summary = client.download_from_content_page(
            page_url=config.page_url,
            output_dir=config.file_path,
            overwrite=config.overwrite,
            max_files=config.max_files,
        )

    logger.info("Downloaded: %s", len(summary.downloaded))
    for item in summary.downloaded:
        logger.info("  - %s (%s bytes)", item.local_path, item.size_bytes)

    logger.info("Skipped: %s", len(summary.skipped))
    for item in summary.skipped:
        logger.info("  - %s | %s", item.source_url, item.reason)

    return 0


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print("Arguments are no longer required. Starting interactive mode...")
    raise SystemExit(main())
