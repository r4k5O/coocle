from __future__ import annotations

import os
from typing import TypedDict

import httpx


GITHUB_REPO = os.environ.get("GITHUB_REPO", "r4k5O/coocle").strip()
GITHUB_API_TOKEN = os.environ.get("GITHUB_API_TOKEN", "").strip()


class GitHubStats(TypedDict):
    stars: int
    forks: int
    open_issues: int
    open_prs: int
    watchers: int
    subscribers: int


async def fetch_github_stats() -> GitHubStats:
    if not GITHUB_REPO:
        raise RuntimeError("GITHUB_REPO ist nicht konfiguriert.")

    headers = {
        "Accept": "application/vnd.github.v3+json",
    }
    if GITHUB_API_TOKEN:
        headers["Authorization"] = f"token {GITHUB_API_TOKEN}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Fetch basic repo info
        repo_response = await client.get(
            f"https://api.github.com/repos/{GITHUB_REPO}",
            headers=headers,
        )
        repo_response.raise_for_status()
        repo_data = repo_response.json()

        # Fetch open issues (including PRs)
        issues_response = await client.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/issues?state=open&per_page=1",
            headers=headers,
        )
        issues_response.raise_for_status()
        issues_count = int(issues_response.headers.get("X-Total-Count", 0))

        # Fetch open PRs separately
        prs_response = await client.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/pulls?state=open&per_page=1",
            headers=headers,
        )
        prs_response.raise_for_status()
        prs_count = int(prs_response.headers.get("X-Total-Count", 0))

        return GitHubStats(
            stars=int(repo_data.get("stargazers_count", 0)),
            forks=int(repo_data.get("forks_count", 0)),
            open_issues=issues_count,
            open_prs=prs_count,
            watchers=int(repo_data.get("subscribers_count", 0)),
            subscribers=int(repo_data.get("subscribers_count", 0)),
        )


GITHUB_STAR_THRESHOLDS = [10, 25, 50, 100, 250, 500, 1000]
GITHUB_FORK_THRESHOLDS = [5, 10, 25, 50, 100, 250, 500]
GITHUB_WATCHER_THRESHOLDS = [5, 10, 25, 50, 100, 250, 500]


def detect_github_milestone(stats: GitHubStats, kind: str, last_milestone: int | None) -> int | None:
    thresholds = []
    if kind == "stars":
        thresholds = GITHUB_STAR_THRESHOLDS
    elif kind == "forks":
        thresholds = GITHUB_FORK_THRESHOLDS
    elif kind == "watchers":
        thresholds = GITHUB_WATCHER_THRESHOLDS
    else:
        return None

    current_value = stats.get(kind, 0) if isinstance(stats, dict) else getattr(stats, kind, 0)

    threshold = 0
    for t in thresholds:
        if t > current_value:
            break
        if last_milestone is not None and t <= last_milestone:
            continue
        threshold = t
    return threshold if threshold > 0 else None
