"""Validation rules for CSV uploads and SQL input."""
import pytest

from app.csvutil import ValidationError, parse_csv, validate_select


def test_valid_csv_basic(make_csv):
    parsed = parse_csv(make_csv([("a", "x", "1"), ("b", "y", "2")]).read())
    assert parsed.header == ["sample_id", "item", "value"]
    assert parsed.rows == [["a", "x", "1"], ["b", "y", "2"]]


def test_duplicate_rows_are_kept(make_csv):
    rows = [("a", "x", "1"), ("a", "x", "1"), ("a", "x", "1")]
    parsed = parse_csv(make_csv(rows).read())
    assert len(parsed.rows) == 3
    assert parsed.rows[0] == parsed.rows[1] == parsed.rows[2]


def test_bad_header_rejected(make_csv):
    with pytest.raises(ValidationError, match="表头"):
        parse_csv(make_csv([("a",)], header=("wrong", "item", "value")).read())


def test_wrong_arity_rejected(make_csv):
    buf = make_csv([("a", "x", "1"), ("b", "only-two")])
    with pytest.raises(ValidationError, match="字段"):
        parse_csv(buf.read())


def test_row_limit_enforced(make_csv):
    rows = [("s%d" % i, "x", "1") for i in range(1001)]
    with pytest.raises(ValidationError, match="1000"):
        parse_csv(make_csv(rows).read())


def test_exactly_1000_rows_accepted(make_csv):
    rows = [("s%d" % i, "x", "1") for i in range(1000)]
    parsed = parse_csv(make_csv(rows).read())
    assert len(parsed.rows) == 1000


def test_non_utf8_rejected():
    with pytest.raises(ValidationError, match="UTF-8"):
        parse_csv(b"sample_id,item,value\n\xfe\xfd, x, 1\n")


def test_select_validation_accepts_select_and_with():
    validate_select("SELECT 1 FROM readings")
    validate_select("  select * from readings  ;  ")
    validate_select("WITH t AS (SELECT * FROM readings) SELECT * FROM t")


@pytest.mark.parametrize("sql", [
    "INSERT INTO readings VALUES ('a','b','c')",
    "SELECT 1; DROP TABLE readings",
    "PRAGMA threads=4",
    "UPDATE readings SET value='1'",
    "DELETE FROM readings",
    "CREATE TABLE t AS SELECT * FROM readings",
    "CALL dbcheck()",
    "ATTACH 'x.db' AS x",
])
def test_non_select_rejected(sql):
    with pytest.raises(ValidationError):
        validate_select(sql)


@pytest.mark.parametrize("sql", [
    "SELECT random() FROM readings",
    "SELECT uuid() FROM readings",
    "SELECT current_timestamp FROM readings",
    "SELECT now() FROM readings",
])
def test_nondeterministic_rejected(sql):
    with pytest.raises(ValidationError, match="确定性"):
        validate_select(sql)


def test_empty_sql_rejected():
    with pytest.raises(ValidationError):
        validate_select("   ")
