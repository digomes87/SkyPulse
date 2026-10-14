from pathlib import Path

import duckdb

from skypulse.site.build import build, euros

GOLD_SQL = Path(__file__).with_name("gold.sql")


def gold_database(tmp_path: Path) -> Path:
    path = tmp_path / "gold.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute(GOLD_SQL.read_text())
    return path


def fake_dbt_target(tmp_path: Path) -> Path:
    target = tmp_path / "target"
    target.mkdir()
    for name in ("index.html", "manifest.json", "catalog.json"):
        (target / name).write_text("{}")
    return target


def test_site_builds_every_page_with_the_gold_numbers(tmp_path: Path) -> None:
    out = tmp_path / "public"

    build(gold_database(tmp_path), fake_dbt_target(tmp_path), out)

    landing = (out / "index.html").read_text()
    insights = (out / "insights" / "index.html").read_text()
    assert "€600" in landing
    assert "Ryanair" in insights
    assert "thunderstorm" in insights
    assert (out / "dbt" / "manifest.json").exists()
    assert (out / "static" / "site.css").exists()
    assert (out / ".nojekyll").exists()


def test_pages_link_assets_relative_to_their_depth(tmp_path: Path) -> None:
    out = tmp_path / "public"

    build(gold_database(tmp_path), None, out)

    assert 'href="static/site.css"' in (out / "index.html").read_text()
    assert 'href="../static/site.css"' in (out / "insights" / "index.html").read_text()


def test_euro_formatting() -> None:
    assert euros(1750) == "€1,750"
