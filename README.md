# PKUAutoNotes

Download files from PKU course content pages to local folders.

## What This Project Does

- Logs in through PKU IAAA SSO.
- Opens a Blackboard course content page.
- Detects probable downloadable resource links.
- Downloads files into a local directory.
- Can auto-discover all available courses and crawl each course content area.

## Quick Start

1. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

2. Start interactive downloader:

```bash
python download_content.py
```

The script stores your latest settings in `pku_autonotes_config.json` (same folder as `download_content.py`) and uses them as defaults next time.

At startup, the script now prints all current config items once, and you only need to confirm once to continue.
If not confirmed, it enters edit mode for manual adjustments.

Key config items:

- `file_path`: download root folder path.
- `auto_add_suffix`: whether to auto append filename extension when missing.

The script will ask for all options interactively, including:

- IAAA username/password (password uses hidden input)
- Single page mode or all-courses mode
- Output directory and overwrite behavior
- Optional limits and keyword filters

In `All courses` mode, the script now reads the homepage module `当前学期课程` and processes only current-term courses by default.
It will locate each course's `教学内容` page and download all coursewares from that page.

For login settings, the script now uses recommended defaults automatically and will try several known Blackboard redirect URLs if one fails.
Only enable custom `appid/redirUrl` when you know exactly what to use.

Optional environment variables for quicker input:

```bash
export PKU_USERNAME=YOUR_STUDENT_ID
export PKU_PASSWORD=YOUR_PASSWORD
python download_content.py
```

Password behavior:

- You can choose to save password into the local config file to avoid repeated input.
- Saved password is plain text; only enable this on a trusted local machine.

Download filename behavior:

- If the downloaded filename has no extension, the script auto-appends a suffix based on URL or `Content-Type`.
- If the type cannot be determined, it appends `.bin`.

Course folder naming behavior:

- Course folders are normalized to `课程名__course_id`.
- Long course-code prefixes and semester suffixes are trimmed for cleaner names.

## Download All Courses

Choose `All courses` in the interactive mode, then set optional limits/filters when prompted.

## Teaching Content Courseware

- In `Single content page` mode, if the page is a Blackboard `教学内容` page, `list only` shows a clean courseware list (`title`, `content_id`, `file_url`).
- Download mode on that page will fetch all coursewares listed in the teaching-content list.

## Useful Options

- In interactive mode you can choose `list only` to preview courses/resources without downloading.
- `max files per page`, `max courses`, and `max pages per course` are all optional prompts.
- `course keyword` can be left blank for no filtering.

## Notes

- This script targets common PKU Blackboard patterns and may need updates if page structure changes.
- Use only on courses you are authorized to access.
