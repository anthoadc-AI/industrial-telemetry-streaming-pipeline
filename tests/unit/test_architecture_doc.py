"""docs/ARCHITECTURE.md must agree with the files it cites (Task 3.4, owner decision A2).

The document's value tables are read under their headings and compared with their source of truth:
- "Topics": names, partitions, replication factor and topic-level settings, with config/topics.yaml (config.loader);
- "Thresholds": channel defaults with config/alarms.yaml, and per-asset motor current with the resolved rules;
- "Settings": each row's dotted setting read from the file in its Source column (mosquitto.conf, YAML, compose);
- "Pinned versions": each row's version text found verbatim in the file in its Source column.
Also checked: every repository path cited in backticks exists (with its line, when one is given), and every
telemetry.* topic name in the document is one config/topics.yaml defines.

Out of scope: the prose. Whether a sentence describes the behaviour correctly is covered by the traceability
table in docs/internal/, not here. The "Broker defaults in effect" table is not checked either: those values
are not in the repository (they were read from a running broker).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from config.loader import load_config

REPO_ROOT = Path(__file__).resolve().parents[2]
DOC = REPO_ROOT / "docs" / "ARCHITECTURE.md"
CODE = re.compile(r"`([^`]+)`")
FENCE = re.compile(r"^```.*?^```", re.S | re.M)
TOPIC = re.compile(r"(?<![\w/])telemetry\.[a-z]+\b")
PATH_LINE = re.compile(r"^(?P<path>[A-Za-z0-9_./-]+?)(?::(?P<line>\d+)(?:-(?P<end>\d+))?)?$")


# --- parsing ------------------------------------------------------------------------


def table_under(text: str, heading: str) -> list[dict[str, str]]:
    """Rows of the first Markdown table after the heading line `heading`, as dicts keyed by the header cells."""
    lines = text.splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if line.strip() == heading)
    except StopIteration:
        raise AssertionError(f"heading {heading!r} not found") from None
    rows: list[list[str]] = []
    for line in lines[start + 1:]:
        if line.startswith("#"):
            break
        if line.startswith("|"):
            rows.append([c.strip() for c in line.strip().strip("|").split("|")])
        elif rows:
            break
    if len(rows) < 3:
        raise AssertionError(f"no table under {heading!r}")
    header, body = rows[0], rows[2:]
    return [dict(zip(header, row, strict=True)) for row in body]


def code(cell: str) -> str:
    """The cell's text without backticks."""
    return cell.replace("`", "").strip()


def number(cell: str) -> float:
    return float(code(cell).split()[0])


def yaml_get(data, dotted: str):
    """Walk a dotted path through nested mappings whose keys may themselves contain dots (enable.idempotence)."""
    rest = dotted
    while rest:
        if not isinstance(data, dict):
            raise KeyError(dotted)
        key = next((k for k in sorted(data, key=lambda k: -len(str(k)))
                    if rest == str(k) or rest.startswith(f"{k}.")), None)
        if key is None:
            raise KeyError(dotted)
        data, rest = data[key], rest[len(str(key)) + 1:]
    return data


def conf_get(text: str, key: str) -> str:
    """A `key value` line of a Mosquitto configuration file."""
    for line in text.splitlines():
        parts = line.split()
        if parts and not parts[0].startswith("#") and parts[0] == key:
            return " ".join(parts[1:])
    raise KeyError(key)


def setting_value(source: Path, setting: str) -> str:
    text = source.read_text(encoding="utf-8")
    if source.suffix == ".conf":
        return conf_get(text, setting)
    value = yaml_get(yaml.safe_load(text), setting)
    return str(value).lower() if isinstance(value, bool) else str(value)


def cited_paths(text: str) -> list[tuple[str, int | None]]:
    """(path, line) for every backticked repository path: its first segment must be a top-level entry of the repo."""
    top = {p.name for p in REPO_ROOT.iterdir()}
    found = []
    for raw in CODE.findall(FENCE.sub("", text)):
        m = PATH_LINE.match(raw)
        if not m or m["path"].split("/")[0] not in top:
            continue
        found.append((m["path"].rstrip("/"), int(m["end"] or m["line"]) if m["line"] else None))
    return found


# --- the document against the repository --------------------------------------------


@pytest.fixture(scope="module")
def doc() -> str:
    return DOC.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def cfg():
    return load_config(env={})


def test_topics_table_matches_config(doc, cfg):
    rows = {code(r["Topic"]): r for r in table_under(doc, "### Topics")}
    topics = {t.name: t for t in cfg.topics.kafka.values()}
    assert set(rows) == set(topics)
    for name, row in rows.items():
        t = topics[name]
        assert int(row["Partitions"]) == t.partitions, name
        assert int(row["Replication factor"]) == t.replication_factor, name
        assert code(row["Value schema"]) == t.value_schema.relative_to(REPO_ROOT).as_posix(), name
        settings = code(row["Topic-level settings"])
        listed = {} if settings == "none" else dict(s.strip().split("=", 1) for s in settings.split(","))
        assert listed == dict(t.configs), name


def test_threshold_tables_match_alarms_config(doc, cfg):
    channels = yaml.safe_load((REPO_ROOT / "config" / "alarms.yaml").read_text(encoding="utf-8"))["channels"]
    rows = {code(r["Channel"]): r for r in table_under(doc, "### Thresholds")}
    assert set(rows) == set(channels)
    for channel, row in rows.items():
        c = channels[channel]
        for column, key in (("Warning", "warning"), ("Critical", "critical")):
            if key in c:
                assert number(row[column]) == c[key], (channel, column)
            else:
                assert row[column] == "per asset", (channel, column)
        assert number(row["Hysteresis"]) == c["hysteresis"], channel
        assert int(row["Persistence (samples)"]) == c["persistence_samples"], channel

    per_asset = {code(r["Asset"]): r for r in table_under(doc, "`motor_current_a` thresholds are set per asset, "
                                                                 "at 105 % and 120 % of rated current:")}
    assert set(per_asset) == {a.id for a in cfg.telemetry.assets}
    for asset, row in per_asset.items():
        rule = cfg.alarms.rule(asset, "motor_current_a")
        assert (number(row["Warning"]), number(row["Critical"])) == (rule.warning, rule.critical), asset


def test_settings_table_matches_its_sources(doc):
    rows = table_under(doc, "### Settings")
    assert rows
    for row in rows:
        source, setting = REPO_ROOT / code(row["Source"]), code(row["Setting"])
        assert setting_value(source, setting) == code(row["Value"]), (setting, code(row["Source"]))


def test_pinned_versions_appear_in_their_sources(doc):
    rows = table_under(doc, "### Pinned versions")
    assert rows
    for row in rows:
        source, version = REPO_ROOT / code(row["Source"]), code(row["Version"])
        assert version in source.read_text(encoding="utf-8"), (row["Component"], version, code(row["Source"]))


def test_every_cited_path_exists(doc):
    cited = cited_paths(doc)
    assert len(cited) > 20
    for path, line in cited:
        target = REPO_ROOT / path
        assert target.exists(), path
        if line is not None:
            assert len(target.read_text(encoding="utf-8").splitlines()) >= line, f"{path}:{line}"


def test_topic_names_in_the_document_exist(doc, cfg):
    names = {t.name for t in cfg.topics.kafka.values()}
    mentioned = set(TOPIC.findall(doc))
    assert mentioned and mentioned <= names, mentioned - names
    blocks = re.findall(r"```mermaid\n(.*?)```", doc, re.S)
    assert len(blocks) == 3
    for block in blocks:
        assert set(TOPIC.findall(block)) <= names


# --- the checks themselves catch drift ------------------------------------------------


TABLE = """## Section
### Settings

| Setting | Value | Source |
|---|---|---|
| `a` | 1 | `x` |

text
"""


def test_table_under_reads_rows_and_stops_at_the_table_end():
    assert table_under(TABLE, "### Settings") == [{"Setting": "`a`", "Value": "1", "Source": "`x`"}]
    with pytest.raises(AssertionError, match="not found"):
        table_under(TABLE, "### Missing")
    with pytest.raises(AssertionError, match="no table"):
        table_under("### Settings\n\nno table here\n## Next\n", "### Settings")


def test_yaml_get_handles_dotted_keys():
    data = {"producer": {"acks": "all", "enable.idempotence": True}, "a": {"b.c": {"d": 1}}}
    assert yaml_get(data, "producer.enable.idempotence") is True
    assert yaml_get(data, "a.b.c.d") == 1
    with pytest.raises(KeyError):
        yaml_get(data, "producer.linger.ms")
    with pytest.raises(KeyError):
        yaml_get(data, "producer.acks.more")


def test_conf_get_ignores_comments():
    text = "# max_queued_messages 1\nmax_queued_messages 60000\n"
    assert conf_get(text, "max_queued_messages") == "60000"
    with pytest.raises(KeyError):
        conf_get(text, "sys_interval")


def test_a_changed_value_is_caught(doc, cfg):
    drifted = doc.replace("| `max_queued_messages` | 60000 |", "| `max_queued_messages` | 50000 |")
    assert drifted != doc
    with pytest.raises(AssertionError):
        test_settings_table_matches_its_sources(drifted)
    drifted = doc.replace("| `telemetry.raw` | `asset_id` | key (default partitioner) | 3 |",
                          "| `telemetry.raw` | `asset_id` | key (default partitioner) | 6 |")
    assert drifted != doc
    with pytest.raises(AssertionError):
        test_topics_table_matches_config(drifted, cfg)


def test_cited_paths_skip_non_repository_text():
    text = ("```mermaid\nA[`x`]\n```\n"
            "`plant/+/telemetry` `config/topics.yaml` `docker-compose.yml` `consumer.js:12` `config/alarms.yaml:3`")
    assert cited_paths(text) == [("config/topics.yaml", None), ("docker-compose.yml", None), ("config/alarms.yaml", 3)]
