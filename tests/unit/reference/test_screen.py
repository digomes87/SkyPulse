from skypulse.reference.screen import ReferenceScreenError, screen

RATIO = 0.9


def test_a_first_load_only_needs_rows() -> None:
    assert screen({"airports": 900, "countries": 0}, {}, RATIO) == ["countries has no rows"]


def test_small_changes_pass() -> None:
    assert screen({"airports": 910}, {"airports": 1000}, RATIO) == []


def test_a_table_that_shrinks_too_much_fails() -> None:
    problems = screen({"airports": 899}, {"airports": 1000}, RATIO)

    assert problems == ["airports has 899 rows, 1000 before (minimum 90%)"]


def test_growth_always_passes() -> None:
    assert screen({"airports": 5000}, {"airports": 1000}, RATIO) == []


def test_the_error_lists_every_problem() -> None:
    error = ReferenceScreenError(["a has no rows", "b has no rows"])

    assert str(error) == "reference data rejected: a has no rows; b has no rows"
