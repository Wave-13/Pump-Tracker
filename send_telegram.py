#!/usr/bin/env python3
"""Send pump leads to Telegram. Sends nothing when there are no leads.

The bot token and chat id come from the environment (GitHub secrets in
Actions) — never put them in this file:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""
import json
import os

import requests


def main():
    try:
        with open("leads.json") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("No leads.json, nothing to send.")
        return
    leads = data.get("leads", [])
    if not leads:
        print("No leads, staying silent.")
        return
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    lines = [f"New pump leads ({len(leads)}):"]
    for lead in leads:
        reason = lead["reasons"][0] if lead.get("reasons") else ""
        lines.append(f"- {lead['name']} (${lead['symbol']}): {reason}")
        lines.append(f"  {lead['pump_link']}")
    text = "\n".join(lines)
    resp = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Sent {len(leads)} lead(s) to Telegram.")


if __name__ == "__main__":
    main()
