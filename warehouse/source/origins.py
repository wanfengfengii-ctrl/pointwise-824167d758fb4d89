from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class Origin:
    document: str
    row: int


def require_text(value, field: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f'{field} must be a nonblank string')
    return value


def parse_origin(value) -> Origin:
    if not isinstance(value, Mapping) or set(value) != {'document', 'row'}:
        raise ValueError('origin requires document and row')
    document = require_text(value['document'], 'origin.document')
    row = value['row']
    if type(row) is not int or row < 1:
        raise ValueError('origin.row must be a positive integer')
    return Origin(document, row)


def encode_origin(origin: Origin) -> dict:
    return {'document': origin.document, 'row': origin.row}
