# TOOLS.md - Local Notes

Skills define _how_ tools work. This file is for _your_ specifics — the stuff that's unique to your setup.

## What Goes Here

Things like:

- Camera names and locations
- SSH hosts and aliases
- Preferred voices for TTS
- Speaker/room names
- Device nicknames
- Anything environment-specific

## Examples

```markdown
### Cameras

- living-room → Main area, 180° wide angle
- front-door → Entrance, motion-triggered

### SSH

- home-server → 192.168.1.100, user: admin

### TTS

- Preferred voice: "Nova" (warm, slightly British)
- Default speaker: Kitchen HomePod
```

## Why Separate?

Skills are shared. Your setup is yours. Keeping them apart means you can update skills without losing your notes, and share skills without leaking your infrastructure.

---

## Factor Lab data services

- Diemeng / 灵启数据 base URL: `https://data.diemeng.chat/api`.
- API key is stored at `runtime/secrets/settings/diemeng_api_key`; never print or commit it.
- Authentication uses the `apiKey` HTTP header.
- Historical stock minutes use `POST /stock/history` with `stock_code`, `level`,
  `start_time`, `end_time`, `page`, and `page_size`.
- Supported levels verified locally: `1min`, `5min`, and `15min` (provider also
  documents `30min` and `60min`). A 2026-09-02 read-only probe found pre-2016
  1min history for six representative listings: `000001.SZ`, `000002.SZ`, and
  `600000.SH` from `2000-06-09`, plus `600519.SH`, `002024.SZ`, and
  `300001.SZ` from their 2001/2004/2009 listing dates. These are ticker-specific
  observations, not a universal market coverage boundary.
- Diemeng volume units vary by ticker/date (`300630.SZ` can already be shares,
  while `000001.SZ` requires hands `*100`). Infer each slice uniquely from
  candidate multipliers via the three consumed A/B/C five-minute aggregate
  VWAPs inside aggregate OHLC; never impose one global unit. Each window must
  contain zero or five exact rows, while every raw row still passes timestamp,
  OHLC geometry, nonnegative amount/volume, and zero-state checks. Exact 1min
  09:30 is opening-price evidence; a zero-volume/amount flat row equal to raw
  pre-close is a provider placeholder and cannot override raw daily open.
  09:36 and 09:42 are decision-overlap rows.
  Treat every execution window as observable only after its final bar.

Add whatever helps you do your job. This is your cheat sheet.
