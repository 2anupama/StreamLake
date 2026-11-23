"""Credential-safe operational alert delivery."""

from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage

from streamlake.common.settings import Settings

logger = logging.getLogger("streamlake.alerts")


def send_alert(subject: str, body: str, settings: Settings | None = None) -> bool:
    """Always log an alert; optionally deliver it through configured SMTP."""
    settings = settings or Settings()
    logger.error("%s: %s", subject, body)
    if not (settings.smtp_host and settings.smtp_from_email and settings.alert_to_email):
        return False
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.smtp_from_email
    message["To"] = settings.alert_to_email
    message.set_content(body)
    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=15) as client:
            if settings.smtp_use_tls:
                client.starttls()
            if settings.smtp_username:
                password = settings.smtp_password.get_secret_value() if settings.smtp_password else None
                client.login(settings.smtp_username, password or "")
            client.send_message(message)
        return True
    except (OSError, smtplib.SMTPException):
        logger.exception("Unable to deliver configured SMTP alert")
        return False
