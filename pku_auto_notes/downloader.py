from __future__ import annotations

from collections import deque
import mimetypes
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

DEFAULT_APP_ID = "blackboard"
DEFAULT_BASE_URL = "https://course.pku.edu.cn"
DEFAULT_PORTAL_HOME_URL = "https://course.pku.edu.cn/webapps/portal/execute/tabs/tabAction?tab_tab_group_id=_1_1"
DEFAULT_REDIR_URL = "http://course.pku.edu.cn/webapps/bb-sso-BBLEARN/execute/authValidate/campusLogin"
DEFAULT_SSO_BRIDGE_URL = "https://course.pku.edu.cn/webapps/bb-sso-BBLEARN/login.html"
DEFAULT_REDIR_URL_CANDIDATES = (
    "http://course.pku.edu.cn/webapps/bb-sso-BBLEARN/execute/authValidate/campusLogin",
    "https://course.pku.edu.cn/webapps/bb-sso-BBLEARN/execute/authValidate/campusLogin",
    "https://course.pku.edu.cn/webapps/bb-sso-BBLEARN/index.jsp",
    "https://course.pku.edu.cn/webapps/login/?action=relogin",
    "https://course.pku.edu.cn/webapps/portal/execute/defaultTab",
    "https://course.pku.edu.cn/",
)
DEFAULT_COURSE_TAB_URLS = (
    "https://course.pku.edu.cn/webapps/portal/execute/defaultTab",
    "https://course.pku.edu.cn/webapps/portal/execute/tabs/tabAction?tab_tab_group_id=_2_1",
    "https://course.pku.edu.cn/webapps/portal/execute/tabs/tabAction?tab_tab_group_id=_1_1",
    "https://course.pku.edu.cn/webapps/login/",
)
IAAA_LOGIN_URL = "https://iaaa.pku.edu.cn/iaaa/oauthlogin.do"
DEFAULT_TIMEOUT = 30
DEFAULT_LOGIN_RETRY_ATTEMPTS = 4
DEFAULT_HISTORY_DB_NAME = ".pku_autonotes_history.db"


@dataclass
class DownloadedFile:
    source_url: str
    local_path: Path
    size_bytes: int


@dataclass
class SkippedFile:
    source_url: str
    reason: str


@dataclass
class DownloadSummary:
    downloaded: list[DownloadedFile]
    skipped: list[SkippedFile]


@dataclass
class CoursewareItem:
    title: str
    file_url: str
    content_id: str | None
    view_url: str | None


@dataclass
class CourseInfo:
    course_id: str
    title: str
    entry_url: str


@dataclass
class CourseDownloadSummary:
    course: CourseInfo
    pages_scanned: int
    downloaded: list[DownloadedFile]
    skipped: list[SkippedFile]


class AlreadyDownloadedError(RuntimeError):
    pass


class PKUCourseDownloader:
    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT,
        auto_add_suffix: bool = True,
        history_db_path: Path | None = None,
    ) -> None:
        self.timeout = timeout
        self.auto_add_suffix = auto_add_suffix
        self.history_db_path = history_db_path
        self._history_conn: sqlite3.Connection | None = None
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                )
            }
        )
        self._init_history_db()

    def _init_history_db(self) -> None:
        if self.history_db_path is None:
            return

        self.history_db_path.parent.mkdir(parents=True, exist_ok=True)
        self._history_conn = sqlite3.connect(self.history_db_path)
        self._history_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS download_history (
                normalized_url TEXT PRIMARY KEY,
                source_url TEXT NOT NULL,
                local_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'downloaded',
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self._history_conn.commit()

    def close(self) -> None:
        if self._history_conn is not None:
            self._history_conn.close()
            self._history_conn = None

    def __del__(self) -> None:
        self.close()

    def login(
        self,
        username: str,
        password: str,
        appid: str = DEFAULT_APP_ID,
        redir_url: str = DEFAULT_REDIR_URL,
    ) -> None:
        discovered_pairs = self._discover_sso_pairs()
        candidates = _build_login_candidates(
            appid=appid,
            redir_url=redir_url,
            discovered_pairs=discovered_pairs,
        )
        last_error = "IAAA login failed"

        for candidate_appid, candidate_redir in candidates:
            payload = {
                "appid": candidate_appid,
                "userName": username,
                "password": password,
                "randCode": "",
                "smsCode": "",
                "otpCode": "",
                "redirUrl": candidate_redir,
            }

            try:
                resp = self._request_with_retry(
                    method="POST",
                    url=IAAA_LOGIN_URL,
                    attempts=DEFAULT_LOGIN_RETRY_ATTEMPTS,
                    data=payload,
                    timeout=self.timeout,
                    headers={"Connection": "close"},
                )
            except requests.exceptions.SSLError as exc:
                raise RuntimeError(_format_ssl_error_message(exc)) from exc
            except requests.RequestException as exc:
                raise RuntimeError(f"Network error while contacting IAAA: {exc}") from exc

            resp.raise_for_status()
            body = resp.json()

            token = body.get("token")
            success = body.get("success")
            if success and token:
                try:
                    sso_resp = self._request_with_retry(
                        method="GET",
                        url=candidate_redir,
                        attempts=DEFAULT_LOGIN_RETRY_ATTEMPTS,
                        params={"token": token},
                        allow_redirects=True,
                        timeout=self.timeout,
                    )
                except requests.exceptions.SSLError as exc:
                    raise RuntimeError(_format_ssl_error_message(exc)) from exc
                except requests.RequestException as exc:
                    raise RuntimeError(f"Network error while completing SSO: {exc}") from exc

                sso_resp.raise_for_status()
                return

            last_error = body.get("errors", {}).get("msg") or body.get("msg") or "IAAA login failed"
            if not _is_sso_parameter_error(last_error):
                raise RuntimeError(last_error)

        tried = ", ".join(f"appid={a}, redirUrl={r}" for a, r in candidates)
        raise RuntimeError(f"{last_error} Tried SSO params: {tried}")

    def _discover_sso_pairs(self) -> list[tuple[str, str]]:
        urls_to_probe = (
            DEFAULT_SSO_BRIDGE_URL,
            "https://course.pku.edu.cn/webapps/login/",
        )
        pairs: list[tuple[str, str]] = []

        for url in urls_to_probe:
            try:
                resp = self._request_with_retry(
                    method="GET",
                    url=url,
                    attempts=2,
                    timeout=self.timeout,
                    allow_redirects=True,
                )
            except requests.RequestException:
                continue

            for appid, redir_url in _extract_sso_pairs_from_html(resp.text):
                if (appid, redir_url) not in pairs:
                    pairs.append((appid, redir_url))

        return pairs

    def _request_with_retry(
        self,
        method: str,
        url: str,
        attempts: int,
        **kwargs: object,
    ) -> requests.Response:
        last_exc: requests.RequestException | None = None
        for attempt in range(1, max(attempts, 1) + 1):
            try:
                return self.session.request(method=method, url=url, **kwargs)
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                last_exc = exc
                if attempt >= attempts:
                    break
                # Simple backoff for transient campus-network or TLS handshake instability.
                time.sleep(0.6 * attempt)

        assert last_exc is not None
        raise last_exc

    def _get_history_path(self, normalized_url: str) -> Path | None:
        if self._history_conn is None:
            return None

        row = self._history_conn.execute(
            "SELECT local_path FROM download_history WHERE normalized_url = ?",
            (normalized_url,),
        ).fetchone()
        if row is None:
            return None

        candidate = Path(row[0])
        if candidate.exists():
            return candidate
        return None

    def _upsert_history(
        self,
        normalized_url: str,
        source_url: str,
        local_path: Path,
        size_bytes: int,
        status: str,
    ) -> None:
        if self._history_conn is None:
            return

        self._history_conn.execute(
            """
            INSERT INTO download_history (normalized_url, source_url, local_path, size_bytes, status, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(normalized_url) DO UPDATE SET
                source_url = excluded.source_url,
                local_path = excluded.local_path,
                size_bytes = excluded.size_bytes,
                status = excluded.status,
                updated_at = CURRENT_TIMESTAMP
            """,
            (normalized_url, source_url, str(local_path), int(size_bytes), status),
        )
        self._history_conn.commit()

    def list_download_targets(self, page_url: str) -> list[tuple[str, str]]:
        html = self._fetch_page(page_url)
        return list(_parse_download_targets(html, page_url))

    def list_teaching_content_coursewares(self, page_url: str) -> list[CoursewareItem]:
        html = self._fetch_page(page_url)
        return _parse_teaching_content_coursewares(html, page_url)

    def find_teaching_content_page(self, course: CourseInfo) -> str | None:
        candidate_pages = [
            course.entry_url,
            _build_course_entry_url(course.course_id),
        ]

        for candidate in candidate_pages:
            try:
                html = self._fetch_page(candidate)
            except requests.RequestException:
                continue

            teaching_url = _extract_teaching_content_page_url(
                html=html,
                base_url=candidate,
                course_id=course.course_id,
            )
            if teaching_url:
                return teaching_url

        return None

    def download_from_teaching_content_page(
        self,
        page_url: str,
        output_dir: Path,
        overwrite: bool = False,
        max_files: int | None = None,
    ) -> DownloadSummary:
        output_dir.mkdir(parents=True, exist_ok=True)
        items = self.list_teaching_content_coursewares(page_url)
        if max_files is not None:
            items = items[: max(max_files, 0)]

        downloaded: list[DownloadedFile] = []
        skipped: list[SkippedFile] = []
        seen_urls: set[str] = set()

        for item in items:
            candidates = [item.file_url]
            if item.view_url and item.view_url != item.file_url:
                candidates.append(item.view_url)

            result: DownloadedFile | None = None
            last_reason: str | None = None
            for candidate in candidates:
                normalized = _normalize_url(candidate)
                if normalized in seen_urls:
                    continue
                seen_urls.add(normalized)

                try:
                    attempt = self._download_one(candidate, item.title, output_dir, overwrite, page_url)
                except AlreadyDownloadedError as exc:
                    last_reason = str(exc)
                    break
                except requests.RequestException as exc:
                    last_reason = f"request error: {exc}"
                    continue
                except OSError as exc:
                    last_reason = f"io error: {exc}"
                    continue

                if attempt is not None:
                    result = attempt
                    break

                last_reason = "not a downloadable file"

            if result is not None:
                downloaded.append(result)
            else:
                source = item.file_url or (item.view_url or page_url)
                skipped.append(SkippedFile(source_url=source, reason=last_reason or "download failed"))

        return DownloadSummary(downloaded=downloaded, skipped=skipped)

    def discover_courses(self) -> list[CourseInfo]:
        found: dict[str, CourseInfo] = {}

        # Primary source: portal home modules often contain both current and history course lists.
        try:
            home_html = self._fetch_page(DEFAULT_PORTAL_HOME_URL)
        except requests.RequestException:
            home_html = ""

        if home_html:
            for course in _parse_homepage_course_links(home_html, DEFAULT_PORTAL_HOME_URL):
                found[course.course_id] = course

        for tab_url in DEFAULT_COURSE_TAB_URLS:
            try:
                html = self._fetch_page(tab_url)
            except requests.RequestException:
                continue

            for course in _parse_course_links(html, tab_url):
                previous = found.get(course.course_id)
                if previous is None:
                    found[course.course_id] = course
                    continue

                # Keep the richest visible title if the same course appears more than once.
                if len(course.title) > len(previous.title):
                    found[course.course_id] = course

            # Fallback: some PKU pages hide course URLs in scripts/JSON without anchor text.
            for course_id in _extract_course_ids_from_html(html):
                if course_id in found:
                    continue
                fallback_url = (
                    f"{DEFAULT_BASE_URL}/webapps/blackboard/execute/courseMain?course_id={course_id}"
                )
                found[course_id] = CourseInfo(
                    course_id=course_id,
                    title=course_id,
                    entry_url=fallback_url,
                )

        courses = sorted(found.values(), key=lambda item: item.title.lower())
        if not courses:
            raise RuntimeError(
                "No courses discovered automatically. The account may have hidden tab layout "
                "or non-standard course links."
            )
        return courses

    def discover_current_term_courses(self) -> list[CourseInfo]:
        try:
            html = self._fetch_page(DEFAULT_PORTAL_HOME_URL)
        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to open portal homepage: {exc}") from exc

        courses = sorted(
            _dedupe_courses(_parse_homepage_course_links(html, DEFAULT_PORTAL_HOME_URL, current_term_only=True)),
            key=lambda item: item.title.lower(),
        )

        if not courses:
            raise RuntimeError(
                "No current-term courses found on homepage module '当前学期课程'. "
                "Please verify account permissions or homepage layout."
            )
        return courses

    def list_course_content_pages(
        self,
        course: CourseInfo,
        max_pages: int = 120,
    ) -> list[str]:
        seeds = [
            course.entry_url,
            f"{DEFAULT_BASE_URL}/webapps/blackboard/execute/courseMain?course_id={course.course_id}",
        ]
        queue: deque[str] = deque(_normalize_url(url) for url in seeds)
        visited: set[str] = set()
        content_pages: set[str] = set()

        max_visited = max(max_pages * 8, 240)
        while queue and len(visited) < max_visited:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)

            try:
                html = self._fetch_page(current)
            except requests.RequestException:
                continue

            if _is_course_content_page(current, course.course_id):
                content_pages.add(current)
                if len(content_pages) >= max_pages:
                    continue

            for next_url in _parse_course_navigation_links(html, current, course.course_id):
                normalized = _normalize_url(next_url)
                if normalized in visited:
                    continue
                queue.append(normalized)

        if not content_pages:
            return [course.entry_url]
        return sorted(content_pages)

    def download_course(
        self,
        course: CourseInfo,
        output_root: Path,
        overwrite: bool = False,
        max_pages: int = 120,
        max_files_per_page: int | None = None,
        teaching_content_only: bool = True,
    ) -> CourseDownloadSummary:
        course_dir_name = _build_concise_course_dir_name(course.title, course.course_id)
        course_dir = output_root / course_dir_name
        course_dir.mkdir(parents=True, exist_ok=True)

        if teaching_content_only:
            teaching_page_url = self.find_teaching_content_page(course)
            if not teaching_page_url:
                return CourseDownloadSummary(
                    course=course,
                    pages_scanned=0,
                    downloaded=[],
                    skipped=[
                        SkippedFile(
                            source_url=course.entry_url,
                            reason="teaching content page not found",
                        )
                    ],
                )

            summary = self.download_from_teaching_content_page(
                page_url=teaching_page_url,
                output_dir=course_dir,
                overwrite=overwrite,
                max_files=max_files_per_page,
            )
            return CourseDownloadSummary(
                course=course,
                pages_scanned=1,
                downloaded=summary.downloaded,
                skipped=summary.skipped,
            )

        pages = self.list_course_content_pages(course=course, max_pages=max_pages)
        downloaded: list[DownloadedFile] = []
        skipped: list[SkippedFile] = []
        seen_targets: set[str] = set()

        for page_url in pages:
            targets = self.list_download_targets(page_url)
            if max_files_per_page is not None:
                targets = targets[: max(max_files_per_page, 0)]

            for source_url, title in targets:
                normalized = _normalize_url(source_url)
                if normalized in seen_targets:
                    continue
                seen_targets.add(normalized)

                try:
                    result = self._download_one(source_url, title, course_dir, overwrite, page_url)
                except AlreadyDownloadedError as exc:
                    skipped.append(SkippedFile(source_url=source_url, reason=str(exc)))
                    continue
                except requests.RequestException as exc:
                    skipped.append(SkippedFile(source_url=source_url, reason=f"request error: {exc}"))
                    continue
                except OSError as exc:
                    skipped.append(SkippedFile(source_url=source_url, reason=f"io error: {exc}"))
                    continue

                if result is None:
                    skipped.append(SkippedFile(source_url=source_url, reason="not a downloadable file"))
                else:
                    downloaded.append(result)

        return CourseDownloadSummary(
            course=course,
            pages_scanned=len(pages),
            downloaded=downloaded,
            skipped=skipped,
        )

    def download_all_courses(
        self,
        output_root: Path,
        overwrite: bool = False,
        max_courses: int | None = None,
        max_pages_per_course: int = 120,
        max_files_per_page: int | None = None,
        course_keyword: str | None = None,
        current_term_only: bool = True,
        teaching_content_only: bool = True,
    ) -> list[CourseDownloadSummary]:
        output_root.mkdir(parents=True, exist_ok=True)
        courses = self.discover_current_term_courses() if current_term_only else self.discover_courses()

        if course_keyword:
            keyword = course_keyword.lower().strip()
            courses = [course for course in courses if keyword in course.title.lower()]

        if max_courses is not None:
            courses = courses[: max(max_courses, 0)]

        results: list[CourseDownloadSummary] = []
        for course in courses:
            result = self.download_course(
                course=course,
                output_root=output_root,
                overwrite=overwrite,
                max_pages=max_pages_per_course,
                max_files_per_page=max_files_per_page,
                teaching_content_only=teaching_content_only,
            )
            results.append(result)

        return results

    def download_from_content_page(
        self,
        page_url: str,
        output_dir: Path,
        overwrite: bool = False,
        max_files: int | None = None,
    ) -> DownloadSummary:
        output_dir.mkdir(parents=True, exist_ok=True)
        targets = self.list_download_targets(page_url)
        if max_files is not None:
            targets = targets[: max(max_files, 0)]

        downloaded: list[DownloadedFile] = []
        skipped: list[SkippedFile] = []

        for source_url, title in targets:
            try:
                result = self._download_one(source_url, title, output_dir, overwrite, page_url)
            except AlreadyDownloadedError as exc:
                skipped.append(SkippedFile(source_url=source_url, reason=str(exc)))
                continue
            except requests.RequestException as exc:
                skipped.append(SkippedFile(source_url=source_url, reason=f"request error: {exc}"))
                continue
            except OSError as exc:
                skipped.append(SkippedFile(source_url=source_url, reason=f"io error: {exc}"))
                continue

            if result is None:
                skipped.append(SkippedFile(source_url=source_url, reason="not a downloadable file"))
            else:
                downloaded.append(result)

        return DownloadSummary(downloaded=downloaded, skipped=skipped)

    def _fetch_page(self, page_url: str) -> str:
        resp = self.session.get(page_url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text

    def _download_one(
        self,
        source_url: str,
        title: str,
        output_dir: Path,
        overwrite: bool,
        referer: str,
    ) -> DownloadedFile | None:
        normalized_source = _normalize_url(source_url)
        history_path = self._get_history_path(normalized_source)
        if history_path is not None and not overwrite:
            raise AlreadyDownloadedError(f"already downloaded: {history_path}")

        resp = self.session.get(
            source_url,
            stream=True,
            allow_redirects=True,
            timeout=self.timeout,
            headers={"Referer": referer},
        )
        resp.raise_for_status()

        content_type = (resp.headers.get("Content-Type") or "").lower()
        content_disposition = resp.headers.get("Content-Disposition") or ""
        is_attachment = "attachment" in content_disposition.lower()

        if "text/html" in content_type and not is_attachment:
            return None

        filename = _pick_filename(resp, title)
        if not filename:
            filename = _filename_from_url(resp.url)
        if not filename:
            filename = "downloaded_file"
        if self.auto_add_suffix:
            filename = _ensure_filename_suffix(
                filename=filename,
                response_url=resp.url,
                content_type=content_type,
            )

        final_path = output_dir / _sanitize_filename(filename)
        if final_path.exists() and not overwrite:
            self._upsert_history(
                normalized_url=normalized_source,
                source_url=source_url,
                local_path=final_path,
                size_bytes=final_path.stat().st_size,
                status="existing",
            )
            raise AlreadyDownloadedError(f"already exists locally: {final_path}")

        final_path = _dedupe_path(final_path, overwrite)
        size_bytes = 0
        with final_path.open("wb") as file_obj:
            for chunk in resp.iter_content(chunk_size=1024 * 64):
                if not chunk:
                    continue
                file_obj.write(chunk)
                size_bytes += len(chunk)

        self._upsert_history(
            normalized_url=normalized_source,
            source_url=source_url,
            local_path=final_path,
            size_bytes=size_bytes,
            status="downloaded",
        )

        return DownloadedFile(source_url=source_url, local_path=final_path, size_bytes=size_bytes)


def _parse_download_targets(html: str, base_url: str) -> Iterable[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()

    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue

        absolute = urljoin(base_url, href)
        normalized = _normalize_url(absolute)
        if normalized in seen:
            continue

        if not _is_probable_download_link(normalized):
            continue

        seen.add(normalized)
        title = anchor.get_text(" ", strip=True) or _filename_from_url(normalized) or "download"
        yield normalized, title


def _parse_teaching_content_coursewares(html: str, base_url: str) -> list[CoursewareItem]:
    soup = BeautifulSoup(html, "html.parser")
    result: list[CoursewareItem] = []
    seen: set[str] = set()

    for li in soup.select("ul#content_listContainer li.liItem"):
        anchor = li.select_one("div.item h3 a[href]")
        if not anchor:
            continue

        href = (anchor.get("href") or "").strip()
        if not href:
            continue

        file_url = urljoin(base_url, href)
        title = anchor.get_text(" ", strip=True) or "courseware"
        content_id = _extract_content_id_from_li(li)

        onclick = anchor.get("onclick") or ""
        view_url = _extract_view_url_from_onclick(onclick, base_url)
        if not content_id and view_url:
            content_id = _extract_content_id_from_url(view_url)

        normalized = _normalize_url(file_url)
        if normalized in seen:
            continue
        seen.add(normalized)

        result.append(
            CoursewareItem(
                title=title,
                file_url=file_url,
                content_id=content_id,
                view_url=view_url,
            )
        )

    return result


def _extract_teaching_content_page_url(html: str, base_url: str, course_id: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    anchors = soup.select("#courseMenuPalette_contents a[href]") or soup.select("a[href]")
    for anchor in anchors:
        text_parts = [
            anchor.get_text(" ", strip=True),
            (anchor.get("title") or "").strip(),
        ]
        span_title = anchor.select_one("span[title]")
        if span_title:
            text_parts.append((span_title.get("title") or "").strip())

        text = " ".join(part for part in text_parts if part)
        if "教学内容" not in text:
            continue

        href = (anchor.get("href") or "").strip()
        if not href:
            continue

        absolute = urljoin(base_url, href)
        lower = absolute.lower()
        if "listcontent.jsp" not in lower:
            continue

        parsed_course_id = _extract_course_id(absolute)
        if parsed_course_id and parsed_course_id != course_id:
            continue
        return absolute

    return None


def _extract_view_url_from_onclick(onclick: str, base_url: str) -> str | None:
    if not onclick:
        return None

    match = re.search(r"['\"](/webapps/blackboard/execute/content/file\?[^'\"]+)['\"]", onclick)
    if not match:
        return None
    return urljoin(base_url, match.group(1))


def _extract_content_id_from_li(li: BeautifulSoup) -> str | None:
    li_id = (li.get("id") or "").strip()
    match = re.search(r"contentListItem:(_[^\s]+_\d+)", li_id)
    if match:
        return match.group(1)

    item_div = li.select_one("div.item[id]")
    if item_div:
        div_id = (item_div.get("id") or "").strip()
        if _looks_like_blackboard_course_id(div_id):
            return div_id

    return None


def _extract_content_id_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key == "content_id" and value:
            return value
    return None


def _build_login_candidates(
    appid: str,
    redir_url: str,
    discovered_pairs: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []

    def append_if_new(candidate_appid: str, candidate_redir: str) -> None:
        normalized_appid = (candidate_appid or "").strip()
        normalized_redir = (candidate_redir or "").strip()
        if not normalized_appid or not normalized_redir:
            return

        pair = (normalized_appid, normalized_redir)
        if pair not in candidates:
            candidates.append(pair)

    append_if_new(appid, redir_url)
    for discovered_appid, discovered_redir in discovered_pairs:
        append_if_new(discovered_appid, discovered_redir)

    if appid.strip().lower() == DEFAULT_APP_ID:
        for candidate_redir in DEFAULT_REDIR_URL_CANDIDATES:
            append_if_new(DEFAULT_APP_ID, candidate_redir)

    return candidates


def _extract_sso_pairs_from_html(html: str) -> list[tuple[str, str]]:
    appid_matches = re.findall(
        r"name=['\"]appID['\"][^>]*value=['\"]([^'\"]+)['\"]",
        html,
        flags=re.IGNORECASE,
    )
    redir_matches = re.findall(
        r"name=['\"]redirectUrl['\"][^>]*value=['\"]([^'\"]+)['\"]",
        html,
        flags=re.IGNORECASE,
    )

    pairs: list[tuple[str, str]] = []
    for appid in appid_matches:
        for redir in redir_matches:
            pair = (appid.strip(), redir.strip())
            if pair not in pairs and pair[0] and pair[1]:
                pairs.append(pair)

    return pairs


def _format_ssl_error_message(exc: BaseException) -> str:
    return (
        "SSL handshake failed when connecting to PKU IAAA. "
        "This is often a transient network/proxy issue. "
        "Please retry, switch network (or connect PKU VPN), and ensure system time is correct. "
        f"Original error: {exc}"
    )


def _is_sso_parameter_error(message: str) -> bool:
    lower = message.lower()
    config_related = (
        ("redirect" in lower and "url" in lower)
        or ("app" in lower and "id" in lower)
    )
    invalid_related = any(word in lower for word in ("not correct", "incorrect", "invalid"))
    return config_related and invalid_related


def _parse_course_links(html: str, base_url: str) -> Iterable[CourseInfo]:
    soup = BeautifulSoup(html, "html.parser")
    seen_pairs: set[tuple[str, str]] = set()

    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue

        absolute = urljoin(base_url, href)
        course_id = _extract_course_id(absolute)
        if not course_id:
            continue

        title = anchor.get_text(" ", strip=True)
        title = _clean_course_title(title) or course_id
        pair = (course_id, absolute)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        yield CourseInfo(course_id=course_id, title=title, entry_url=_build_course_entry_url(course_id))

    for embedded_url in _extract_embedded_urls(html, base_url):
        course_id = _extract_course_id(embedded_url)
        if not course_id:
            continue

        pair = (course_id, embedded_url)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        yield CourseInfo(course_id=course_id, title=course_id, entry_url=_build_course_entry_url(course_id))


def _parse_homepage_course_links(
    html: str,
    base_url: str,
    current_term_only: bool = False,
) -> list[CourseInfo]:
    soup = BeautifulSoup(html, "html.parser")
    found: list[CourseInfo] = []

    for module in soup.select("div.portlet"):
        title_elem = module.select_one("span.moduleTitle")
        module_title = title_elem.get_text(" ", strip=True) if title_elem else ""
        if not module_title:
            continue

        if current_term_only and not _is_current_term_module_title(module_title):
            continue

        has_course_list = bool(module.select_one("ul.courseListing"))
        if not has_course_list:
            continue

        for anchor in module.select("ul.courseListing a[href]"):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue

            absolute = urljoin(base_url, href)
            course_id = _extract_course_id(absolute)
            if not course_id:
                continue

            title = _clean_course_title(anchor.get_text(" ", strip=True)) or course_id
            found.append(
                CourseInfo(
                    course_id=course_id,
                    title=title,
                    entry_url=_build_course_entry_url(course_id),
                )
            )

    return _dedupe_courses(found)


def _parse_course_navigation_links(
    html: str,
    base_url: str,
    course_id: str,
) -> Iterable[str]:
    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue

        absolute = urljoin(base_url, href)
        if not _is_navigable_course_link(absolute, course_id):
            continue

        yield absolute


def _is_probable_download_link(url: str) -> bool:
    lower = url.lower()
    if any(part in lower for part in ("logout", "javascript:", "mailto:")):
        return False

    # Blackboard and common file-hosting URL patterns.
    if any(part in lower for part in ("/bbcswebdav/", "/xid-", "/download?", "/content/")):
        return True

    path = urlparse(lower).path
    return bool(re.search(r"\.[a-z0-9]{2,8}$", path))


def _is_navigable_course_link(url: str, course_id: str) -> bool:
    lower = url.lower()
    if any(part in lower for part in ("javascript:", "mailto:", "logout")):
        return False

    parsed = urlparse(url)
    if parsed.netloc and parsed.netloc != urlparse(DEFAULT_BASE_URL).netloc:
        return False

    extracted = _extract_course_id(url)
    if extracted and extracted != course_id:
        return False

    path = parsed.path.lower()
    return any(
        part in path
        for part in (
            "/webapps/blackboard/content/",
            "/webapps/blackboard/execute/coursemain",
            "/webapps/blackboard/execute/launcher",
        )
    )


def _is_course_content_page(url: str, course_id: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    extracted = _extract_course_id(url)
    if extracted and extracted != course_id:
        return False

    return (
        "/webapps/blackboard/content/" in path
        and any(part in path for part in ("listcontent", "content"))
    )


def _extract_course_id(url: str) -> str | None:
    parsed = urlparse(url)
    query = parse_qsl(parsed.query, keep_blank_values=True)
    query_dict = {key: value for key, value in query}

    direct_keys = ("course_id", "courseid", "context_id")
    for key, value in query:
        if key in direct_keys and value:
            return value

    # Blackboard launcher frequently uses id=_xxxx_1 with type=Course.
    launcher_id = query_dict.get("id")
    launcher_type = (query_dict.get("type") or "").lower()
    if launcher_id and launcher_type == "course" and _looks_like_blackboard_course_id(launcher_id):
        return launcher_id

    if launcher_id:
        decoded_launcher_id = unquote(launcher_id)
        pk_match = re.search(r"key=(_[^,}\s]+_\d+)", decoded_launcher_id)
        if pk_match:
            return pk_match.group(1)

    ultra_match = re.search(r"/ultra/courses/([^/?#]+)", parsed.path, flags=re.IGNORECASE)
    if ultra_match:
        ultra_id = ultra_match.group(1)
        if ultra_id:
            return ultra_id

    # Some URLs nest a full URL in query values, so recursively parse decoded values.
    for _, value in query:
        if "course_id=" not in value:
            continue
        nested = unquote(value)
        nested_match = re.search(r"course_id=([^&]+)", nested)
        if nested_match:
            return nested_match.group(1)

    match = re.search(r"course_id=([^&]+)", url)
    if match:
        return match.group(1)

    generic_id_match = re.search(r"(?:^|[?&])id=(_[^&]+_\d+)", url)
    if generic_id_match:
        return generic_id_match.group(1)

    return None


def _extract_embedded_urls(html: str, base_url: str) -> list[str]:
    patterns = (
        r"https?://course\.pku\.edu\.cn[^\"'<>\s]+",
        r"/webapps/[^\"'<>\s]+",
    )
    urls: list[str] = []
    for pattern in patterns:
        for match in re.findall(pattern, html, flags=re.IGNORECASE):
            candidate = urljoin(base_url, match)
            if candidate not in urls:
                urls.append(candidate)
    return urls


def _extract_course_ids_from_html(html: str) -> set[str]:
    course_ids: set[str] = set()

    for match in re.findall(r"course_id=(_[^&\"'\s]+_\d+)", html, flags=re.IGNORECASE):
        course_ids.add(match)

    for match in re.findall(r'"course_id"\s*:\s*"(_[^\"]+_\d+)"', html, flags=re.IGNORECASE):
        course_ids.add(match)

    for match in re.findall(r"(?:^|[?&])id=(_[^&\"'\s]+_\d+)", html, flags=re.IGNORECASE):
        if _looks_like_blackboard_course_id(match):
            course_ids.add(match)

    return course_ids


def _looks_like_blackboard_course_id(value: str) -> bool:
    return bool(re.fullmatch(r"_[^_]+_\d+", value))


def _is_current_term_module_title(title: str) -> bool:
    lower = title.lower().strip()
    return (
        "当前学期" in title
        or "本学期" in title
        or "current term" in lower
        or "current semester" in lower
        or "current courses" in lower
    )


def _dedupe_courses(courses: Iterable[CourseInfo]) -> list[CourseInfo]:
    found: dict[str, CourseInfo] = {}
    for course in courses:
        previous = found.get(course.course_id)
        if previous is None or len(course.title) > len(previous.title):
            found[course.course_id] = course
    return list(found.values())


def _build_course_entry_url(course_id: str) -> str:
    return f"{DEFAULT_BASE_URL}/webapps/blackboard/execute/courseMain?course_id={course_id}"


def _build_concise_course_dir_name(title: str, course_id: str) -> str:
    clean_title = _clean_course_title(title) or course_id
    clean_title = re.sub(r"\s+", " ", clean_title).strip()
    max_title_len = 22
    if len(clean_title) > max_title_len:
        clean_title = clean_title[:max_title_len].rstrip()

    short_course_id = course_id.strip("_").split("_")[0] if course_id else "course"
    return _sanitize_filename(f"{clean_title}_{short_course_id}")


def _clean_course_title(raw_title: str) -> str:
    text = re.sub(r"\s+", " ", raw_title).strip()
    text = re.sub(r"^(课程|Course)\s*[:：-]?\s*", "", text, flags=re.IGNORECASE)

    # PKU Blackboard may prefix course title with one or multiple coded segments.
    # Handles both "..._ 哲学导论" and "..._哲学导论" styles.
    coded_prefix = re.match(r"^(?:[0-9A-Za-z-]+_+)+(.+)$", text)
    if coded_prefix:
        text = coded_prefix.group(1).strip()

    # Additional cleanup for residual coded fragment like "265-00-1_哲学导论".
    text = re.sub(r"^[0-9A-Za-z-]{3,}_+\s*", "", text)

    # If Chinese title exists, prefer content starting from first Chinese character.
    first_cjk = re.search(r"[\u4e00-\u9fff]", text)
    if first_cjk:
        text = text[first_cjk.start():].strip()

    text = re.sub(r"\([^)]*学期[^)]*\)$", "", text).strip()
    text = re.sub(r"（[^）]*学期[^）]*）$", "", text).strip()
    return text


def _normalize_url(url: str) -> str:
    parsed = urlparse(url)
    query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, ""))


def _pick_filename(resp: requests.Response, title: str) -> str:
    disposition = resp.headers.get("Content-Disposition") or ""
    # Handles both filename= and RFC 5987 filename*= forms.
    for pattern in (
        r"filename\*=UTF-8''([^;]+)",
        r'filename\*=\"?UTF-8\'\'([^\";]+)',
        r'filename=\"([^\"]+)\"',
        r"filename=([^;]+)",
    ):
        match = re.search(pattern, disposition, flags=re.IGNORECASE)
        if match:
            return requests.utils.unquote(match.group(1).strip().strip('"'))

    return title.strip()


def _filename_from_url(url: str) -> str:
    path = urlparse(url).path
    if not path:
        return ""
    name = Path(path).name
    return name.strip()


def _ensure_filename_suffix(filename: str, response_url: str, content_type: str) -> str:
    current = Path(filename)
    if current.suffix:
        return filename

    url_name = _filename_from_url(response_url)
    if url_name and Path(url_name).suffix:
        return f"{filename}{Path(url_name).suffix}"

    mime = (content_type or "").split(";", 1)[0].strip().lower()
    if mime:
        preferred = {
            "application/pdf": ".pdf",
            "application/msword": ".doc",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
            "application/vnd.ms-powerpoint": ".ppt",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
            "application/vnd.ms-excel": ".xls",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
            "text/plain": ".txt",
            "text/csv": ".csv",
            "application/zip": ".zip",
        }
        suffix = preferred.get(mime) or mimetypes.guess_extension(mime)
        if suffix == ".jpe":
            suffix = ".jpg"
        if suffix:
            return f"{filename}{suffix}"

    return f"{filename}.bin"


def _sanitize_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|\r\n\t]", "_", name).strip()
    return name[:180] if name else "downloaded_file"


def _dedupe_path(path: Path, overwrite: bool) -> Path:
    if overwrite or not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    for idx in range(1, 10_000):
        candidate = path.with_name(f"{stem}_{idx}{suffix}")
        if not candidate.exists():
            return candidate

    raise OSError("could not find an available filename")
