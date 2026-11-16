"""CSV parsing/validation and deterministic-SELECT validation."""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import List, Tuple

EXPECTED_HEADER = ["sample_id", "item", "value"]
MAX_ROWS = 1000
MAX_FILE_BYTES = 10 * 1024 * 1024  # 10 MiB upload cap


class ValidationError(ValueError):
    """Raised when user-supplied input does not meet the contract."""


@dataclass
class ParsedCsv:
    header: List[str]
    rows: List[List[str]]
    raw_text: str
    line_terminator: str


def parse_csv(content: bytes) -> ParsedCsv:
    """Validate an uploaded inspection CSV and return its rows.

    Rules:
      * exactly the header ``sample_id,item,value`` (case sensitive, order
        sensitive);
      * at most :data:`MAX_ROWS` data rows;
      * every data row must have exactly three text fields;
      * blank lines are not accepted as data.
    """
    if len(content) > MAX_FILE_BYTES:
        raise ValidationError(f"文件超过 {MAX_FILE_BYTES // (1024 * 1024)} MiB 上限")

    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError("CSV 必须是 UTF-8 编码文本") from exc

    if not text.strip():
        raise ValidationError("上传内容为空")

    # Detect the original line terminator so downloads preserve it.
    sample = text[:4096]
    if "\r\n" in sample:
        line_terminator = "\r\n"
    else:
        line_terminator = "\n"

    reader = csv.reader(io.StringIO(text))
    try:
        rows = list(reader)
    except csv.Error as exc:
        raise ValidationError(f"CSV 解析失败：{exc}") from exc

    if not rows:
        raise ValidationError("CSV 中没有任何行")

    header = rows[0]
    if header != EXPECTED_HEADER:
        raise ValidationError(
            "表头必须是且只能是 sample_id,item,value（顺序、大小写一致），"
            f"实际为：{','.join(header)}"
        )

    data_rows = rows[1:]
    # csv.reader represents a trailing empty line as []; drop purely empty
    # records but reject malformed (wrong arity) ones.
    cleaned: List[List[str]] = []
    for idx, row in enumerate(data_rows, start=2):
        if row == []:
            continue
        if len(row) != 3:
            raise ValidationError(
                f"第 {idx} 行有 {len(row)} 个字段，应为 3 个"
            )
        cleaned.append(row)

    if len(cleaned) > MAX_ROWS:
        raise ValidationError(
            f"数据行 {len(cleaned)} 行，超过上限 {MAX_ROWS} 行"
        )
    if not cleaned:
        raise ValidationError("CSV 只有表头，没有数据行")

    return ParsedCsv(
        header=list(EXPECTED_HEADER),
        rows=cleaned,
        raw_text=text,
        line_terminator=line_terminator,
    )


def render_subset_csv(
    rows: List[List[str]],
    header: List[str] = None,
    line_terminator: str = "\r\n",
) -> str:
    """Render a subset keeping the original header, row order and duplicates."""
    header = header or EXPECTED_HEADER
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator=line_terminator)
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue()


def validate_select(sql: str) -> str:
    """Validate that the query is a single deterministic SELECT statement.

    Only a single SELECT (optionally WITH ... SELECT) is accepted.  Multiple
    statements, PRAGMA / ATTACH / COPY / INSERT and other side-effecting or
    nondeterministic constructs are rejected before any trial runs.
    """
    if not sql or not sql.strip():
        raise ValidationError("查询不能为空")
    if len(sql) > 100_000:
        raise ValidationError("查询文本过长（上限 100,000 字符）")

    stripped = sql.strip().rstrip(";").strip()
    if ";" in stripped:
        raise ValidationError("只允许单条 SELECT 语句，检测到语句分隔符 ';'")

    head = stripped.lstrip("(").lstrip().upper()
    if not (head.startswith("SELECT") or head.startswith("WITH")):
        raise ValidationError("查询必须是 SELECT（或以 WITH 开头的单个 SELECT）")

    # Reject obvious side-effecting / environment-dependent statements even
    # when hidden inside CTEs. DuckDB itself rejects most of these, but a
    # clear up-front message is friendlier.
    forbidden = [
        " PRAGMA ",
        " ATTACH ",
        " DETACH ",
        " COPY ",
        " INSERT ",
        " UPDATE ",
        " DELETE ",
        " CREATE ",
        " DROP ",
        " ALTER ",
        " CALL ",
        " EXPORT ",
        " LOAD ",
    ]
    upper = " " + stripped.upper() + " "
    for kw in forbidden:
        if kw in upper:
            raise ValidationError(f"只允许只读 SELECT，检测到禁用关键字：{kw.strip()}")

    # Determinism guard: nondeterministic functions would make the
    # "same exception" property meaningless across trials.
    nondeterministic = [
        "RANDOM(",
        "UUID(",
        "CURRENT_TIMESTAMP",
        "NOW(",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "GET_CURRENT_TIMESTAMP",
        "CURRENT_SCHEMA(",
        "CURRENT_DATABASE(",
    ]
    for fn in nondeterministic:
        if fn in upper:
            raise ValidationError(
                f"查询必须是确定性的，检测到非确定性函数/表达式：{fn.rstrip('(')}"
            )

    return stripped
