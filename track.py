#!/usr/bin/env python3
"""Pump.fun new-launch lead tracker (v1).

Listens to PumpPortal's public new-token stream for a short window,
pulls each launch's socials from its metadata, and flags the launches
most likely to need big wave's services:

  Strict (v2, zero noise): a launch only counts as a lead if it shows a
  real project surface, not just a link:

  - COMMUNITY BOT/AGENT FIT: has a Telegram chat AND a dedicated X
    profile or a website.
  - DISCORD BUILD FIT: has a website with no Discord invite found on it
    (best-effort check) AND a Telegram chat or dedicated X profile.

  Single-tweet X links, same-project duplicate mints, and launches with no socials
  are skipped as noise.
Read-only: no trading, no wallet, no keys.
"""

import argparse
import asyncio
import json
import re
import sys
import time
from datetime import datetime, timezone

import requests
from websockets.asyncio.client import connect

PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"
DISCORD_RE = re.compile(r"discord\.gg/|discord\.com/invite", re.I)
UA = {"User-Agent": "pump-lead-tracker/1.0"}


def is_profile_x(url):
    """True if the X link is a project profile, not a single tweet/status."""
    if not url:
        return False
    url = url.strip().rstrip("/")
    if "/status/" in url:
        return False
    return bool(re.match(r"^https?://(x\.com|twitter\.com)/[A-Za-z0-9_]+$",
                         url, re.I))


def is_community_telegram(url):
    """True if the Telegram link looks like a community group, not a bot.

    Telegram bot usernames must end in 'bot'; a bot link is a tool, not a
    chat full of members to manage, so it does not count as community fit.
    """
    if not url:
        return False
    handle = url.strip().rstrip("/").rsplit("/", 1)[-1]
    return not handle.lower().endswith("bot")


GATEWAYS = [
    "https://gateway.pinata.cloud/ipfs/",
    "https://ipfs.io/ipfs/",
    "https://dweb.link/ipfs/",
    "https://w3s.link/ipfs/",
]


def fetch_metadata(uri):
    """Fetch a token metadata JSON, trying IPFS gateways in order.

    ipfs.io and friends now 429 public gateway requests (they moved to a
    service-worker gateway model); Pinata's gateway answers from this VM.
    """
    if not uri:
        return {}
    m = re.search(r"/ipfs/([A-Za-z0-9]+)", uri)
    if uri.startswith("ipfs://"):
        m = re.search(r"ipfs://([A-Za-z0-9]+)", uri)
    if m:
        cid = m.group(1)
        for gw in GATEWAYS:
            try:
                data = requests.get(gw + cid, headers=UA, timeout=8).json()
                if isinstance(data, dict):
                    return data
            except Exception:
                continue
        return {}
    try:
        data = requests.get(uri, headers=UA, timeout=8).json()
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def pick(d, keys):
    for k in keys:
        v = d.get(k)
        if v:
            return v
    return None


def enrich(event):
    """Pull socials for one launch event. Returns a lead dict."""
    mint = event.get("mint")
    meta = fetch_metadata(event.get("uri"))
    twitter = pick(event, ["twitter"]) or pick(meta, ["twitter", "x"])
    telegram = pick(event, ["telegram"]) or pick(meta, ["telegram"])
    website = pick(event, ["website"]) or pick(meta, ["website"])

    discord_status = "unknown"
    if website:
        try:
            html = requests.get(website, headers=UA, timeout=6).text
            discord_status = "found" if DISCORD_RE.search(html) else "not_found"
        except Exception:
            discord_status = "unknown"

    reasons = []
    dedicated_x = is_profile_x(twitter)
    community_chat = telegram and is_community_telegram(telegram)
    if community_chat and (dedicated_x or website):
        reasons.append("COMMUNITY BOT/AGENT FIT (strong): Telegram chat + real project presence")
    if website and discord_status == "not_found" and (telegram or dedicated_x):
        reasons.append("DISCORD BUILD FIT (strong): site shows no Discord invite (best-effort check)")
    if not reasons:
        return None  # noise: weak surface only (status links, no socials, unconfirmed)

    return {
        "name": event.get("name"),
        "symbol": event.get("symbol"),
        "mint": mint,
        "pump_link": f"https://pump.fun/coin/{mint}" if mint else None,
        "creator": event.get("traderPublicKey") or event.get("creator"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "twitter": twitter,
        "telegram": telegram,
        "website": website,
        "discord_status": discord_status,
        "reasons": reasons,
    }


async def collect(seconds, max_tokens):
    """Collect raw new-token events for `seconds` or until `max_tokens`."""
    events = []
    deadline = time.time() + seconds
    async with connect(PUMPPORTAL_WS, max_size=10 * 1024 * 1024,
                       open_timeout=30) as ws:
        await ws.send(json.dumps({"method": "subscribeNewToken"}))
        while time.time() < deadline and len(events) < max_tokens:
            timeout = max(0.1, deadline - time.time())
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                break
            try:
                data = json.loads(msg)
            except Exception:
                continue
            if data.get("mint"):
                events.append(data)
    return events


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=90)
    ap.add_argument("--max", type=int, default=50)
    ap.add_argument("--out", default="leads.json")
    args = ap.parse_args()

    print(f"Listening for new pump.fun launches ({args.seconds}s, max {args.max})...",
          flush=True)
    events = asyncio.run(collect(args.seconds, args.max))
    print(f"Caught {len(events)} launches. Enriching...", flush=True)

    leads, skipped = [], 0
    seen_socials = set()
    for ev in events:
        lead = enrich(ev)
        if lead is None:
            skipped += 1
            continue
        socials = {s.lower() for s in
                   (lead["telegram"], lead["website"], lead["twitter"]) if s}
        if socials and socials & seen_socials:
            skipped += 1  # same project, another mint
            continue
        seen_socials |= socials
        leads.append(lead)

    with open(args.out, "w") as f:
        json.dump({"run_at": datetime.now(timezone.utc).isoformat(),
                   "caught": len(events), "skipped_noise": skipped,
                   "leads": leads}, f, indent=2)

    print(f"\n{len(leads)} leads ({skipped} skipped as noise). Saved to {args.out}\n")
    for lead in leads:
        print(f"{lead['name']} (${lead['symbol']})")
        print(f"  pump: {lead['pump_link']}")
        if lead["twitter"]:
            print(f"  X: {lead['twitter']}")
        if lead["telegram"]:
            print(f"  Telegram: {lead['telegram']}")
        if lead["website"]:
            print(f"  Site: {lead['website']} (discord: {lead['discord_status']})")
        for r in lead["reasons"]:
            print(f"  -> {r}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
