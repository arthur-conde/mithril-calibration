"""Aggregate community calibration contributions for Samwise, Arwen, and Smaug.

Stdlib-only. Two modes:
  python aggregate.py                          # merge contributions/ -> aggregated/ + README
  python aggregate.py --ingest-issue <path>    # parse a GitHub event, write one contribution, then merge

The --ingest-issue path overwrites the payload's `submitter` field with the GitHub
event's author login before the file is written — anti-spoofing is load-bearing.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CONTRIB_ROOT = REPO_ROOT / "contributions"
AGGREGATED_ROOT = REPO_ROOT / "aggregated"
README_PATH = REPO_ROOT / "README.md"

SAMWISE = "samwise"
ARWEN = "arwen"
SMAUG = "smaug"

EXPECTED_SCHEMA_VERSION = {SAMWISE: 1, ARWEN: 2, SMAUG: 1}

SAMWISE_RATE_DICTS = ("rates", "phaseRates")
SAMWISE_SLOT_DICTS = ("slotCapRates",)
ARWEN_RATE_DICTS = ("itemRates", "signatureRates", "npcRates", "keywordRates")
SMAUG_ABSOLUTE_DICT = "absoluteRates"
SMAUG_RATIO_DICT = "ratioRates"

CONTRIBUTORS_START = "<!-- contributors:start -->"
CONTRIBUTORS_END = "<!-- contributors:end -->"

EXIT_OK = 0
EXIT_UNPARSEABLE_JSON = 10
EXIT_MISSING_SCHEMA_VERSION = 11
EXIT_WRONG_SCHEMA_VERSION = 12
EXIT_WRONG_MODULE = 13
EXIT_MISSING_MODULE = 14
EXIT_MISSING_PAYLOAD = 15
EXIT_UNSUPPORTED_EVENT = 16

log = logging.getLogger("aggregate")


class ValidationError(Exception):
    def __init__(self, exit_code: int, message: str):
        super().__init__(message)
        self.exit_code = exit_code
        self.message = message


def dumps_stable(obj) -> str:
    return json.dumps(obj, sort_keys=True, indent=2, ensure_ascii=False) + "\n"


def write_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def validate_payload(data, expected_module: str) -> None:
    if not isinstance(data, dict):
        raise ValidationError(EXIT_UNPARSEABLE_JSON, "payload is not a JSON object")
    if "schemaVersion" not in data:
        raise ValidationError(EXIT_MISSING_SCHEMA_VERSION, "payload is missing `schemaVersion`")
    if "module" not in data:
        raise ValidationError(EXIT_MISSING_MODULE, "payload is missing `module`")
    expected_version = EXPECTED_SCHEMA_VERSION[expected_module]
    if data["schemaVersion"] != expected_version:
        raise ValidationError(
            EXIT_WRONG_SCHEMA_VERSION,
            f"payload has schemaVersion={data['schemaVersion']!r}; expected {expected_version} for {expected_module}",
        )
    if data["module"] != expected_module:
        raise ValidationError(
            EXIT_WRONG_MODULE,
            f"payload has module={data['module']!r}; expected {expected_module!r}",
        )


def _known_top_level_keys(module: str) -> set[str]:
    common = {"schemaVersion", "module", "exportedAt", "contributorNote", "submitter", "attributionOptOut", "aggregatedAt"}
    if module == SAMWISE:
        return common | set(SAMWISE_RATE_DICTS) | set(SAMWISE_SLOT_DICTS)
    if module == SMAUG:
        return common | {SMAUG_ABSOLUTE_DICT, SMAUG_RATIO_DICT}
    return common | set(ARWEN_RATE_DICTS)


def _log_unknown_fields(filename: str, module: str, data: dict) -> None:
    known = _known_top_level_keys(module)
    unknown = [k for k in data.keys() if k not in known]
    if unknown:
        log.info("%s: ignoring unknown top-level fields: %s", filename, sorted(unknown))


def _merge_rate_entries(entries: list[dict], value_key: str, min_key: str, max_key: str) -> dict:
    total_samples = sum(e.get("sampleCount", 0) for e in entries)
    if total_samples <= 0:
        avg = sum(e.get(value_key, 0) for e in entries) / max(len(entries), 1)
    else:
        avg = sum(e.get(value_key, 0) * e.get("sampleCount", 0) for e in entries) / total_samples
    mins = [e[min_key] for e in entries if min_key in e]
    maxs = [e[max_key] for e in entries if max_key in e]
    result = {value_key: avg, "sampleCount": total_samples}
    if mins:
        result[min_key] = min(mins)
    if maxs:
        result[max_key] = max(maxs)
    return result


def _merge_slot_cap_entries(entries: list[dict]) -> dict:
    observed = [e["observedMax"] for e in entries if "observedMax" in e]
    total_samples = sum(e.get("sampleCount", 0) for e in entries)
    result = {"sampleCount": total_samples}
    if observed:
        result["observedMax"] = max(observed)
    return result


def _merge_dict(dict_key: str, contributions: list[dict], value_key: str, min_key: str, max_key: str) -> dict:
    by_key: dict[str, list[dict]] = {}
    for c in contributions:
        d = c.get(dict_key) or {}
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if isinstance(v, dict):
                by_key.setdefault(k, []).append(v)
    return {k: _merge_rate_entries(v, value_key, min_key, max_key) for k, v in by_key.items()}


def _merge_slot_cap_dict(dict_key: str, contributions: list[dict]) -> dict:
    by_key: dict[str, list[dict]] = {}
    for c in contributions:
        d = c.get(dict_key) or {}
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if isinstance(v, dict):
                by_key.setdefault(k, []).append(v)
    return {k: _merge_slot_cap_entries(v) for k, v in by_key.items()}


def _load_contributions(module: str, contrib_root: Path) -> list[dict]:
    folder = contrib_root / module
    if not folder.exists():
        return []
    out = []
    for path in sorted(folder.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            log.warning("%s: skipping — unparseable JSON (%s)", path.name, e)
            continue
        try:
            validate_payload(data, module)
        except ValidationError as e:
            log.warning("%s: skipping — %s", path.name, e.message)
            continue
        _log_unknown_fields(path.name, module, data)
        out.append(data)
    return out


def aggregate_module(module: str, contrib_root: Path, aggregated_root: Path, now: datetime) -> bool:
    contributions = _load_contributions(module, contrib_root)
    agg: dict = {"schemaVersion": EXPECTED_SCHEMA_VERSION[module], "module": module}

    if module == SAMWISE:
        agg["rates"] = _merge_dict("rates", contributions, "avgSeconds", "minSeconds", "maxSeconds")
        agg["phaseRates"] = _merge_dict("phaseRates", contributions, "avgSeconds", "minSeconds", "maxSeconds")
        agg["slotCapRates"] = _merge_slot_cap_dict("slotCapRates", contributions)
    elif module == SMAUG:
        agg[SMAUG_ABSOLUTE_DICT] = _merge_dict(SMAUG_ABSOLUTE_DICT, contributions, "avgPrice", "minPrice", "maxPrice")
        agg[SMAUG_RATIO_DICT] = _merge_dict(SMAUG_RATIO_DICT, contributions, "avgRatio", "minRatio", "maxRatio")
    else:
        for dk in ARWEN_RATE_DICTS:
            agg[dk] = _merge_dict(dk, contributions, "rate", "minRate", "maxRate")

    out_path = aggregated_root / f"{module}.json"
    existing = None
    if out_path.exists():
        try:
            existing = json.loads(out_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = None

    preserved_timestamp = None
    if existing is not None:
        existing_stripped = {k: v for k, v in existing.items() if k != "aggregatedAt"}
        if existing_stripped == agg:
            preserved_timestamp = existing.get("aggregatedAt")

    agg["aggregatedAt"] = preserved_timestamp or now.strftime("%Y-%m-%dT%H:%M:%SZ")

    new_content = dumps_stable(agg)
    old_content = out_path.read_text(encoding="utf-8") if out_path.exists() else ""
    if new_content == old_content:
        return False
    write_atomic(out_path, new_content)
    return True


def _collect_contributors(contrib_root: Path) -> dict[str, set[str]]:
    by_login: dict[str, set[str]] = {}
    for module in (SAMWISE, ARWEN, SMAUG):
        folder = contrib_root / module
        if not folder.exists():
            continue
        for path in sorted(folder.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            try:
                validate_payload(data, module)
            except ValidationError:
                continue
            if data.get("attributionOptOut") is True:
                continue
            login = data.get("submitter")
            if not login or not isinstance(login, str):
                continue
            by_login.setdefault(login, set()).add(module)
    return by_login


def _render_contributors_block(by_login: dict[str, set[str]]) -> str:
    if not by_login:
        return "_(No contributors yet — be the first!)_"
    lines = []
    for login in sorted(by_login.keys(), key=str.casefold):
        modules = ", ".join(sorted(by_login[login]))
        lines.append(f"- [@{login}](https://github.com/{login}) — {modules}")
    return "\n".join(lines)


def rebuild_contributors_block(readme_path: Path, contrib_root: Path) -> bool:
    text = readme_path.read_text(encoding="utf-8")
    by_login = _collect_contributors(contrib_root)
    inner = _render_contributors_block(by_login)
    pattern = re.compile(
        re.escape(CONTRIBUTORS_START) + r"\n.*?\n" + re.escape(CONTRIBUTORS_END),
        re.DOTALL,
    )
    replacement = f"{CONTRIBUTORS_START}\n{inner}\n{CONTRIBUTORS_END}"
    new_text, n = pattern.subn(replacement, text, count=1)
    if n == 0:
        log.warning("README contributors markers not found; skipping")
        return False
    if new_text == text:
        return False
    write_atomic(readme_path, new_text)
    return True


def run_aggregate(
    contrib_root: Path = CONTRIB_ROOT,
    aggregated_root: Path = AGGREGATED_ROOT,
    readme_path: Path = README_PATH,
    now: datetime | None = None,
) -> None:
    now = now or datetime.now(timezone.utc)
    for module in (SAMWISE, ARWEN, SMAUG):
        aggregate_module(module, contrib_root, aggregated_root, now)
    if readme_path.exists():
        rebuild_contributors_block(readme_path, contrib_root)


JSON_FENCE_RE = re.compile(r"```(?:json)?\s*\n(?P<body>.*?)\n```", re.DOTALL | re.IGNORECASE)


def extract_payload_from_issue_body(body: str) -> dict:
    if body is None:
        raise ValidationError(EXIT_MISSING_PAYLOAD, "issue body is empty")
    candidates = JSON_FENCE_RE.findall(body)
    for candidate in candidates:
        candidate = candidate.strip()
        if not candidate:
            continue
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise ValidationError(EXIT_UNPARSEABLE_JSON, "could not find a valid JSON object in a code fence in the issue body")


CHECKBOX_OPT_OUT_RE = re.compile(
    r"-\s*\[\s*(?P<mark>[xX ])\s*\]\s*Don'?t list me in the README contributors block",
)


def extract_opt_out_from_issue_body(body: str) -> bool:
    if not body:
        return False
    m = CHECKBOX_OPT_OUT_RE.search(body)
    if not m:
        return False
    return m.group("mark").lower() == "x"


def _resolve_event(event: dict) -> tuple[int, str, str, str]:
    if "issue" in event and event["issue"]:
        issue = event["issue"]
        number = int(issue["number"])
        login = issue.get("user", {}).get("login") or ""
        body = issue.get("body") or ""
        labels = issue.get("labels") or []
        title = issue.get("title") or ""
    elif "pull_request" in event and event["pull_request"]:
        pr = event["pull_request"]
        number = int(pr["number"])
        login = pr.get("user", {}).get("login") or ""
        body = pr.get("body") or ""
        labels = pr.get("labels") or []
        title = pr.get("title") or ""
    else:
        raise ValidationError(EXIT_UNSUPPORTED_EVENT, "event payload has neither `issue` nor `pull_request`")

    label_names = {lbl.get("name", "") if isinstance(lbl, dict) else str(lbl) for lbl in labels}
    module = None
    if SAMWISE in label_names:
        module = SAMWISE
    elif ARWEN in label_names:
        module = ARWEN
    elif SMAUG in label_names:
        module = SMAUG
    elif "[samwise-contribution]" in title.lower() or title.lower().startswith("[samwise"):
        module = SAMWISE
    elif "[arwen-contribution]" in title.lower() or title.lower().startswith("[arwen"):
        module = ARWEN
    elif "[smaug-contribution]" in title.lower() or title.lower().startswith("[smaug"):
        module = SMAUG
    if module is None:
        raise ValidationError(EXIT_UNSUPPORTED_EVENT, "could not determine module from labels or title")
    if not login:
        raise ValidationError(EXIT_UNSUPPORTED_EVENT, "event author login is missing")
    return number, login, body, module


def ingest_issue(
    event_path: Path,
    contrib_root: Path = CONTRIB_ROOT,
) -> Path:
    event = json.loads(event_path.read_text(encoding="utf-8"))
    number, login, body, module = _resolve_event(event)
    payload = extract_payload_from_issue_body(body)
    validate_payload(payload, module)

    payload["submitter"] = login
    payload["attributionOptOut"] = extract_opt_out_from_issue_body(body)

    dest = contrib_root / module / f"{number}.json"
    write_atomic(dest, dumps_stable(payload))
    return dest


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingest-issue", type=Path, default=None,
                        help="Path to a GitHub event payload JSON; ingest then aggregate.")
    parser.add_argument("--contributions-root", type=Path, default=CONTRIB_ROOT)
    parser.add_argument("--aggregated-root", type=Path, default=AGGREGATED_ROOT)
    parser.add_argument("--readme", type=Path, default=README_PATH)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    if args.ingest_issue is not None:
        try:
            dest = ingest_issue(args.ingest_issue, contrib_root=args.contributions_root)
        except ValidationError as e:
            print(e.message)
            return e.exit_code
        log.info("wrote %s", dest)

    run_aggregate(
        contrib_root=args.contributions_root,
        aggregated_root=args.aggregated_root,
        readme_path=args.readme,
    )
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
