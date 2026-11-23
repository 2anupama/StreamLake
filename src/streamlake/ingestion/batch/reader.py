"""Chunk-aware CSV and Excel readers for batch source files."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from streamlake.ingestion.batch.schemas import normalize_column_name


@dataclass(frozen=True)
class InputChunk:
    """Parsed records and malformed CSV rows found while reading one chunk."""

    frame: pd.DataFrame
    malformed_rows: tuple[tuple[str, ...], ...] = ()


def validate_extension(path: Path) -> str:
    """Return the normalized supported extension or raise a clear error."""
    extension = path.suffix.lower()
    if extension not in {".csv", ".xlsx", ".xlsm", ".xls"}:
        raise ValueError(f"Unsupported file extension {extension!r}; use CSV or Excel")
    return extension


def read_column_names(path: Path) -> list[str]:
    """Read only the header row for schema validation, including empty sheets."""
    extension = validate_extension(path)
    if extension == ".csv":
        if path.stat().st_size == 0:
            return []
        try:
            columns = pd.read_csv(path, nrows=0, engine="python", encoding="utf-8-sig").columns
        except pd.errors.EmptyDataError:
            return []
    elif extension in {".xlsx", ".xlsm"}:
        from openpyxl import load_workbook

        workbook = load_workbook(path, read_only=True, data_only=True)
        try:
            worksheet = workbook.active
            first_row = next(worksheet.iter_rows(min_row=1, max_row=1, values_only=True), ())
            columns = list(first_row)
        finally:
            workbook.close()
    else:
        import xlrd

        workbook = xlrd.open_workbook(path, on_demand=True)
        try:
            worksheet = workbook.sheet_by_index(0)
            columns = worksheet.row_values(0) if worksheet.nrows else []
        finally:
            workbook.release_resources()

    normalized = [normalize_column_name(column) for column in columns]
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"File {path.name!r} contains duplicate column names")
    return normalized


def iter_input_chunks(path: Path, chunk_size: int) -> Iterator[InputChunk]:
    """Yield source rows in bounded dataframes; CSV parser errors are retained."""
    extension = validate_extension(path)
    if chunk_size < 1:
        raise ValueError("chunk_size must be greater than zero")
    if not path.exists():
        raise FileNotFoundError(path)
    if extension == ".csv":
        yield from _iter_csv_chunks(path, chunk_size)
    elif extension in {".xlsx", ".xlsm"}:
        yield from _iter_openpyxl_chunks(path, chunk_size)
    else:
        yield from _iter_xls_chunks(path, chunk_size)


def _iter_csv_chunks(path: Path, chunk_size: int) -> Iterator[InputChunk]:
    malformed: list[tuple[str, ...]] = []

    def capture_bad_line(fields: list[str]) -> None:
        malformed.append(tuple(fields))
        return None

    try:
        reader = pd.read_csv(
            path,
            chunksize=chunk_size,
            dtype="string",
            keep_default_na=False,
            engine="python",
            encoding="utf-8-sig",
            on_bad_lines=capture_bad_line,
        )
        with reader:
            for frame in reader:
                frame.columns = [normalize_column_name(column) for column in frame.columns]
                found = tuple(malformed)
                malformed.clear()
                yield InputChunk(frame, found)
    except pd.errors.EmptyDataError:
        return

    if malformed:
        yield InputChunk(pd.DataFrame(), tuple(malformed))


def _iter_openpyxl_chunks(path: Path, chunk_size: int) -> Iterator[InputChunk]:
    from openpyxl import load_workbook

    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        worksheet = workbook.active
        rows = worksheet.iter_rows(values_only=True)
        header = next(rows, ())
        columns = [normalize_column_name(column) for column in header]
        batch: list[tuple[object, ...]] = []
        for row in rows:
            batch.append(row)
            if len(batch) == chunk_size:
                yield InputChunk(pd.DataFrame.from_records(batch, columns=columns))
                batch.clear()
        if batch:
            yield InputChunk(pd.DataFrame.from_records(batch, columns=columns))
    finally:
        workbook.close()


def _iter_xls_chunks(path: Path, chunk_size: int) -> Iterator[InputChunk]:
    import xlrd

    workbook = xlrd.open_workbook(path, on_demand=True)
    try:
        worksheet = workbook.sheet_by_index(0)
        columns = [normalize_column_name(column) for column in worksheet.row_values(0)]
        batch: list[list[object]] = []
        for row_index in range(1, worksheet.nrows):
            row: list[object] = []
            for column_index in range(worksheet.ncols):
                cell = worksheet.cell(row_index, column_index)
                if cell.ctype == xlrd.XL_CELL_DATE:
                    row.append(xlrd.xldate_as_datetime(cell.value, workbook.datemode))
                else:
                    row.append(cell.value)
            batch.append(row)
            if len(batch) == chunk_size:
                yield InputChunk(pd.DataFrame.from_records(batch, columns=columns))
                batch.clear()
        if batch:
            yield InputChunk(pd.DataFrame.from_records(batch, columns=columns))
    finally:
        workbook.release_resources()