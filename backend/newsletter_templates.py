from __future__ import annotations

import base64
from pathlib import Path
from typing import TypedDict

ROOT = Path(__file__).resolve().parents[1]
LOGO_PATH = ROOT / "coocle_logo.png"


class MilestoneTemplate(TypedDict):
    subject: str
    html: str
    text: str


def _logo_base64() -> str:
    if LOGO_PATH.is_file():
        return base64.b64encode(LOGO_PATH.read_bytes()).decode("ascii")
    return ""


def _base_html_style() -> str:
    return """
    <style>
      body { margin: 0; padding: 0; background: #f4f6f8; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
      .wrapper { max-width: 600px; margin: 0 auto; background: #ffffff; border-radius: 12px; overflow: hidden; }
      .header { background: linear-gradient(135deg, #0d1b2a 0%, #1b2d45 100%); padding: 32px 28px; text-align: center; }
      .header img { width: 64px; height: 64px; border-radius: 12px; }
      .header h1 { color: #ffffff; margin: 12px 0 4px; font-size: 22px; letter-spacing: -0.02em; }
      .header p { color: #8bb4d0; margin: 0; font-size: 14px; }
      .body { padding: 28px; }
      .body h2 { color: #0d1b2a; font-size: 20px; margin: 0 0 12px; }
      .body p { color: #3a4a5c; font-size: 15px; line-height: 1.6; margin: 0 0 16px; }
      .stat-row { display: table; width: 100%; margin: 20px 0; }
      .stat-cell { display: table-cell; text-align: center; padding: 16px 8px; background: #f0f7fc; border-radius: 8px; }
      .stat-value { font-size: 28px; font-weight: 700; color: #0fa2cb; }
      .stat-label { font-size: 12px; color: #6b8299; text-transform: uppercase; letter-spacing: 0.06em; margin-top: 4px; }
      .cta { display: inline-block; background: #0fa2cb; color: #ffffff; text-decoration: none; padding: 12px 28px; border-radius: 8px; font-weight: 600; font-size: 15px; margin: 8px 0 20px; }
      .footer { padding: 20px 28px; border-top: 1px solid #e8ecf0; text-align: center; }
      .footer p { color: #8b9baa; font-size: 12px; margin: 4px 0; }
      .footer a { color: #0fa2cb; }
    </style>
  """


def _wrap_html(body: str, subtitle: str = "") -> str:
    logo_b64 = _logo_base64()
    logo_src = f"data:image/png;base64,{logo_b64}" if logo_b64 else ""
    return f"""
<!DOCTYPE html>
<html lang="de">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">{_base_html_style()}</head>
<body>
  <div class="wrapper">
    <div class="header">
      {'<img src="' + logo_src + '" alt="Coocle Logo">' if logo_src else ''}
      <h1>Coocle</h1>
      <p>{subtitle}</p>
    </div>
    <div class="body">
      {body}
    </div>
    <div class="footer">
      <p>Coocle &middot; Unabhaengiges Open-Source-Projekt</p>
      <p><a href="https://github.com/r4k5O/Coocle">Quellcode auf GitHub</a></p>
      <p><a href="https://coocle-ctp8.onrender.com/unsubscribe.html">Newsletter abmelden</a></p>
    </div>
  </div>
</body>
</html>"""


def milestone_pages(page_count: int) -> MilestoneTemplate:
    subject = f"Coocle hat {page_count:,} Seiten indexiert!"
    html_body = f"""
      <h2>Neuer Meilenstein erreicht!</h2>
      <p>Coocle hat einen neuen Meilenstein erreicht: <strong>{page_count:,} Seiten</strong> sind jetzt durchsuchbar.</p>
      <div class="stat-row">
        <div class="stat-cell">
          <div class="stat-value">{page_count:,}</div>
          <div class="stat-label">Indexierte Seiten</div>
        </div>
      </div>
      <p>Jede neue Seite macht die Suche besser. Danke, dass du Coocle nutzt!</p>
      <a href="https://coocle-ctp8.onrender.com" class="cta">Jetzt suchen</a>
    """
    text = (
        f"Coocle hat {page_count:,} Seiten indexiert!\n\n"
        f"Jede neue Seite macht die Suche besser. Danke, dass du Coocle nutzt!\n\n"
        f"Jetzt suchen: https://coocle-ctp8.onrender.com\n"
    )
    return MilestoneTemplate(
        subject=subject,
        html=_wrap_html(html_body, subtitle="Meilenstein erreicht"),
        text=text,
    )


def milestone_subscribers(subscriber_count: int) -> MilestoneTemplate:
    subject = f"Coocle hat {subscriber_count:,} Newsletter-Abonnenten!"
    html_body = f"""
      <h2>Die Community waechst!</h2>
      <p>Coocle hat jetzt <strong>{subscriber_count:,} Newsletter-Abonnenten</strong>. Danke fuer dein Interesse!</p>
      <div class="stat-row">
        <div class="stat-cell">
          <div class="stat-value">{subscriber_count:,}</div>
          <div class="stat-label">Abonnenten</div>
        </div>
      </div>
      <p>Gemeinsam bauen wir eine unabhaengige Suchmaschine auf.</p>
      <a href="https://coocle-ctp8.onrender.com" class="cta">Coocle ausprobieren</a>
    """
    text = (
        f"Coocle hat {subscriber_count:,} Newsletter-Abonnenten!\n\n"
        f"Gemeinsam bauen wir eine unabhaengige Suchmaschine auf.\n\n"
        f"Coocle ausprobieren: https://coocle-ctp8.onrender.com\n"
    )
    return MilestoneTemplate(
        subject=subject,
        html=_wrap_html(html_body, subtitle="Community-Update"),
        text=text,
    )


def milestone_feature(feature_name: str, description: str) -> MilestoneTemplate:
    subject = f"Neues Coocle-Feature: {feature_name}"
    html_body = f"""
      <h2>Neues Feature: {feature_name}</h2>
      <p>{description}</p>
      <a href="https://coocle-ctp8.onrender.com" class="cta">Jetzt ausprobieren</a>
    """
    text = (
        f"Neues Coocle-Feature: {feature_name}\n\n"
        f"{description}\n\n"
        f"Jetzt ausprobieren: https://coocle-ctp8.onrender.com\n"
    )
    return MilestoneTemplate(
        subject=subject,
        html=_wrap_html(html_body, subtitle="Feature-Update"),
        text=text,
    )


MILESTONE_PAGE_THRESHOLDS = [100, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000]
MILESTONE_SUBSCRIBER_THRESHOLDS = [10, 25, 50, 100, 250, 500, 1000, 2500, 5000]
ANNIVERSARY_THRESHOLDS = [1, 2, 3, 5, 10]  # Years


def detect_page_milestone(current_count: int, last_milestone: int | None) -> int | None:
    threshold = 0
    for t in MILESTONE_PAGE_THRESHOLDS:
        if t > current_count:
            break
        if last_milestone is not None and t <= last_milestone:
            continue
        threshold = t
    return threshold if threshold > 0 else None


def detect_subscriber_milestone(current_count: int, last_milestone: int | None) -> int | None:
    threshold = 0
    for t in MILESTONE_SUBSCRIBER_THRESHOLDS:
        if t > current_count:
            break
        if last_milestone is not None and t <= last_milestone:
            continue
        threshold = t
    return threshold if threshold > 0 else None


def detect_anniversary(subscribed_at: str, last_anniversary: int | None) -> int | None:
    """Detect if subscriber has reached an anniversary milestone."""
    from datetime import datetime
    try:
        sub_date = datetime.fromisoformat(subscribed_at.replace("Z", "+00:00"))
        now = datetime.now(sub_date.tzinfo)
        years = (now - sub_date).days // 365
    except Exception:
        return None
    
    if years <= 0:
        return None
    
    for threshold in ANNIVERSARY_THRESHOLDS:
        if years == threshold:
            if last_anniversary is None or threshold > last_anniversary:
                return threshold
    return None


def milestone_github_stars(star_count: int, forks: int, open_prs: int) -> MilestoneTemplate:
    subject = f"Coocle hat {star_count:,} GitHub-Stars!"
    html_body = f"""
      <h2>Community auf GitHub waechst!</h2>
      <p>Coocle hat jetzt <strong>{star_count:,} GitHub-Stars</strong>. Danke fuer deine Unterstuetzung!</p>
      <div class="stat-row">
        <div class="stat-cell">
          <div class="stat-value">{star_count:,}</div>
          <div class="stat-label">Stars</div>
        </div>
        <div class="stat-cell">
          <div class="stat-value">{forks:,}</div>
          <div class="stat-label">Forks</div>
        </div>
        <div class="stat-cell">
          <div class="stat-value">{open_prs:,}</div>
          <div class="stat-label">Open PRs</div>
        </div>
      </div>
      <p>Stelle Coocle vor, indem du uns auf GitHub einen Stern gibst!</p>
      <a href="https://github.com/r4k5O/coocle" class="cta">Coocle auf GitHub</a>
    """
    text = (
        f"Coocle hat {star_count:,} GitHub-Stars!\n\n"
        f"Stats: {forks:,} Forks, {open_prs:,} offene PRs\n\n"
        f"Coocle auf GitHub: https://github.com/r4k5O/coocle\n"
    )
    return MilestoneTemplate(
        subject=subject,
        html=_wrap_html(html_body, subtitle="GitHub-Meilenstein"),
        text=text,
    )


def milestone_github_forks(fork_count: int, stars: int) -> MilestoneTemplate:
    subject = f"Coocle hat {fork_count:,} GitHub-Forks!"
    html_body = f"""
      <h2>Coocle wird geforkt!</h2>
      <p>Coocle wurde <strong>{fork_count:,} Mal</strong> auf GitHub geforkt. Entwickler bauen darauf auf!</p>
      <div class="stat-row">
        <div class="stat-cell">
          <div class="stat-value">{fork_count:,}</div>
          <div class="stat-label">Forks</div>
        </div>
        <div class="stat-cell">
          <div class="stat-value">{stars:,}</div>
          <div class="stat-label">Stars</div>
        </div>
      </div>
      <p>Vielen Dank an alle Contributors!</p>
      <a href="https://github.com/r4k5O/coocle" class="cta">Coocle auf GitHub</a>
    """
    text = (
        f"Coocle hat {fork_count:,} GitHub-Forks!\n\n"
        f"Vielen Dank an alle Contributors!\n\n"
        f"Coocle auf GitHub: https://github.com/r4k5O/coocle\n"
    )
    return MilestoneTemplate(
        subject=subject,
        html=_wrap_html(html_body, subtitle="GitHub-Meilenstein"),
        text=text,
    )


def welcome_email(name: str | None = None) -> MilestoneTemplate:
    subject = "Willkommen beim Coocle-Newsletter!"
    greeting = f"Hallo {name}!" if name else "Hallo!"
    html_body = f"""
      <h2>Willkommen beim Coocle-Newsletter!</h2>
      <p>{greeting} Danke, dass du dich fuer den Coocle-Newsletter angemeldet hast.</p>
      <p>Coocle ist eine unabhaengige Open-Source-Suchmaschine. Du wirst Updates ueber neue Features, Meilensteine und wichtige Neuigkeiten erhalten.</p>
      <p>Wenn du keine E-Mails mehr erhalten moechtest, kannst du dich jederzeit abmelden:</p>
      <a href="https://coocle-ctp8.onrender.com/unsubscribe.html" class="cta">Newsletter abmelden</a>
    """
    text = (
        f"{greeting} Danke, dass du dich fuer den Coocle-Newsletter angemeldet hast.\n\n"
        f"Coocle ist eine unabhaengige Open-Source-Suchmaschine. Du wirst Updates ueber neue Features, Meilensteine und wichtige Neuigkeiten erhalten.\n\n"
        f"Wenn du keine E-Mails mehr erhalten moechtest, kannst du dich jederzeit abmelden:\n"
        f"https://coocle-ctp8.onrender.com/unsubscribe.html\n"
    )
    return MilestoneTemplate(
        subject=subject,
        html=_wrap_html(html_body, subtitle="Willkommen"),
        text=text,
    )


def anniversary_email(years: int, name: str | None = None) -> MilestoneTemplate:
    subject = f"{years} Jahre Coocle-Newsletter!"
    greeting = f"Hallo {name}!" if name else "Hallo!"
    html_body = f"""
      <h2>{years} Jahre dabei!</h2>
      <p>{greeting} Du bist seit {years} Jahren Teil des Coocle-Newsletters. Danke fuer deine Treue!</p>
      <p>In den letzten {years} Jahren hat Coocle sich weiterentwickelt und ist zu einer besseren Suchmaschine geworden. Das waere ohne dich und andere Abonnenten nicht moeglich.</p>
      <p>Wir freuen uns auf weitere Jahre mit dir!</p>
      <a href="https://coocle-ctp8.onrender.com" class="cta">Coocle besuchen</a>
    """
    text = (
        f"{greeting} Du bist seit {years} Jahren Teil des Coocle-Newsletters. Danke fuer deine Treue!\n\n"
        f"In den letzten {years} Jahren hat Coocle sich weiterentwickelt und ist zu einer besseren Suchmaschine geworden.\n\n"
        f"Wir freuen uns auf weitere Jahre mit dir!\n\n"
        f"Coocle besuchen: https://coocle-ctp8.onrender.com\n"
    )
    return MilestoneTemplate(
        subject=subject,
        html=_wrap_html(html_body, subtitle=f"{years}. Jubiläum"),
        text=text,
    )
