from pydantic import BaseModel, Field

from cosmos.ai.formatting import format_diagnosis


class Issue(BaseModel):
    file: str
    problem: str


def test_format_diagnosis_puts_confidence_in_the_header_and_each_field_in_a_section():
    class Diagnosis(BaseModel):
        root_cause: str
        suggested_fix: str
        confidence: str

    rendered = format_diagnosis(
        Diagnosis(root_cause="Column foo is missing.", suggested_fix="Rename it to bar.", confidence="high")
    )

    assert rendered.splitlines()[:3] == ["-" * 80, "AI diagnosis (confidence: high)", "-" * 80]
    assert "Root cause:\n  Column foo is missing." in rendered
    assert "Suggested fix:\n  Rename it to bar." in rendered
    assert rendered.endswith("-" * 80)


def test_format_diagnosis_without_confidence_has_a_plain_header():
    class Custom(BaseModel):
        summary: str = Field(description="what happened")

    rendered = format_diagnosis(Custom(summary="Something broke."))

    assert rendered.splitlines()[1] == "AI diagnosis"
    assert "Summary:\n  Something broke." in rendered


def test_format_diagnosis_wraps_long_text():
    class Custom(BaseModel):
        summary: str

    rendered = format_diagnosis(Custom(summary="word " * 100))

    assert all(len(line) <= 88 for line in rendered.splitlines())


def test_format_diagnosis_renders_lists_and_dicts_one_item_per_line():
    class Location(BaseModel):
        file: str
        line: int

    class Custom(BaseModel):
        steps: list[str]
        no_steps: list[str]
        location: Location

    rendered = format_diagnosis(
        Custom(steps=["Rename it.", "Re-run."], no_steps=[], location=Location(file="a.sql", line=3))
    )

    assert "Steps:\n  - Rename it.\n  - Re-run." in rendered
    assert "No steps:\n  (none)" in rendered
    assert "Location:\n  file: a.sql\n  line: 3" in rendered
    assert "[" not in rendered and "{" not in rendered


def test_format_diagnosis_renders_nested_models_inside_lists():
    class Custom(BaseModel):
        issues: list[Issue]
        tags: dict[str, list[str]]

    rendered = format_diagnosis(
        Custom(
            issues=[
                Issue(file="models/a.sql", problem="column x missing"),
                Issue(file="models/b.sql", problem="bad join"),
            ],
            tags={"area": ["schema", "sql"], "empty": []},
        )
    )

    assert (
        "Issues:\n"
        "  - file: models/a.sql\n"
        "    problem: column x missing\n"
        "  - file: models/b.sql\n"
        "    problem: bad join\n"
    ) in rendered
    assert "Tags:\n  area:\n    - schema\n    - sql\n  empty: (none)" in rendered
    # No Python reprs of dicts or models leak into the log.
    assert "{'" not in rendered and "Issue(" not in rendered


def test_format_diagnosis_renders_empty_containers_as_none():
    class Custom(BaseModel):
        labels: dict[str, str]
        steps: list[str]

    rendered = format_diagnosis(Custom(labels={}, steps=[]))

    assert "Labels:\n  (none)" in rendered
    assert "Steps:\n  (none)" in rendered


def test_format_diagnosis_keeps_the_lines_and_indentation_of_a_snippet():
    class Custom(BaseModel):
        suggested_fix: str

    snippet = "Change the model to:\nselect\n    id,\n    amount\nfrom {{ ref('base') }}\n\nThen rerun."

    lines = format_diagnosis(Custom(suggested_fix=snippet)).splitlines()

    assert "  select" in lines
    assert "      id," in lines  # the 4 spaces of the snippet, under the 2 of the section
    assert "      amount" in lines
    assert "  from {{ ref('base') }}" in lines
    assert lines[lines.index("  from {{ ref('base') }}") + 1] == ""  # the blank line between paragraphs


def test_format_diagnosis_does_not_split_long_tokens():
    class Custom(BaseModel):
        root_cause: str

    table = "analytics_prod_warehouse.customer_orders_enriched_with_a_very_long_name_for_the_sake_of_the_test_v2"
    url = "https://docs.getdbt.com/reference/resource-configs/" + "x" * 80

    rendered = format_diagnosis(Custom(root_cause=f"See {table} and {url} for details."))

    assert table in rendered
    assert url in rendered


def test_format_diagnosis_wraps_each_line_of_a_multiline_text_on_its_own():
    class Custom(BaseModel):
        root_cause: str

    long_line = " ".join(["word"] * 60)

    lines = format_diagnosis(Custom(root_cause=f"{long_line}\nshort")).splitlines()

    assert "  short" in lines
    assert all(len(line) <= 88 for line in lines)
