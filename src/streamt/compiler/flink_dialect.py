"""Flink SQL dialect for sqlglot.

This module defines a custom Flink SQL dialect for sqlglot to properly parse
and handle Flink-specific SQL patterns including:
- TUMBLE, HOP, SESSION windowing functions
- TUMBLE_START, TUMBLE_END, etc. time attribute functions
- PROCTIME() for processing time
- Flink-specific data types
"""

from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


class FlinkDialect(Dialect):
    """Custom Flink SQL dialect for sqlglot.

    This dialect extends sqlglot's default dialect to support Flink SQL syntax,
    including windowing functions and Flink-specific types.
    """

    class Tokenizer(Tokenizer):
        """Flink SQL tokenizer with custom keywords."""

        QUOTES = ["'", '"']
        IDENTIFIERS = ["`"]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            # Flink-specific types
            "STRING": TokenType.VARCHAR,
            "BYTES": TokenType.VARBINARY,
            "TINYINT": TokenType.TINYINT,
            "SMALLINT": TokenType.SMALLINT,
            "INT": TokenType.INT,
            "INTEGER": TokenType.INT,
            "BIGINT": TokenType.BIGINT,
            "FLOAT": TokenType.FLOAT,
            "DOUBLE": TokenType.DOUBLE,
            "DECIMAL": TokenType.DECIMAL,
            "BOOLEAN": TokenType.BOOLEAN,
            "DATE": TokenType.DATE,
            "TIME": TokenType.TIME,
            "TIMESTAMP": TokenType.TIMESTAMP,
            "TIMESTAMP_LTZ": TokenType.TIMESTAMPTZ,
            "ARRAY": TokenType.ARRAY,
            "MAP": TokenType.MAP,
            "ROW": TokenType.STRUCT,
            "MULTISET": TokenType.ARRAY,
        }

    class Parser(Parser):
        """Flink SQL parser with custom function support."""

        FUNCTIONS = {
            **Parser.FUNCTIONS,
            # Map Flink window functions to Anonymous (sqlglot doesn't have these)
            # They will be parsed as exp.Anonymous which is fine for type inference
            "TUMBLE": lambda args: exp.Anonymous(this="TUMBLE", expressions=args),
            "TUMBLE_START": lambda args: exp.Anonymous(this="TUMBLE_START", expressions=args),
            "TUMBLE_END": lambda args: exp.Anonymous(this="TUMBLE_END", expressions=args),
            "TUMBLE_ROWTIME": lambda args: exp.Anonymous(this="TUMBLE_ROWTIME", expressions=args),
            "TUMBLE_PROCTIME": lambda args: exp.Anonymous(this="TUMBLE_PROCTIME", expressions=args),
            "HOP": lambda args: exp.Anonymous(this="HOP", expressions=args),
            "HOP_START": lambda args: exp.Anonymous(this="HOP_START", expressions=args),
            "HOP_END": lambda args: exp.Anonymous(this="HOP_END", expressions=args),
            "HOP_ROWTIME": lambda args: exp.Anonymous(this="HOP_ROWTIME", expressions=args),
            "HOP_PROCTIME": lambda args: exp.Anonymous(this="HOP_PROCTIME", expressions=args),
            "SESSION": lambda args: exp.Anonymous(this="SESSION", expressions=args),
            "SESSION_START": lambda args: exp.Anonymous(this="SESSION_START", expressions=args),
            "SESSION_END": lambda args: exp.Anonymous(this="SESSION_END", expressions=args),
            "SESSION_ROWTIME": lambda args: exp.Anonymous(this="SESSION_ROWTIME", expressions=args),
            "SESSION_PROCTIME": lambda args: exp.Anonymous(this="SESSION_PROCTIME", expressions=args),
            # Processing time
            "PROCTIME": lambda args: exp.Anonymous(this="PROCTIME", expressions=args),
            # Flink built-in functions
            "TO_TIMESTAMP": exp.TsOrDsToTimestamp.from_arg_list,
            "FROM_UNIXTIME": exp.UnixToTime.from_arg_list,
            "UNIX_TIMESTAMP": exp.TimeToUnix.from_arg_list,
            "DATE_FORMAT": exp.TimeToStr.from_arg_list,
            "CONCAT": exp.Concat.from_arg_list,
            "CONCAT_WS": exp.ConcatWs.from_arg_list,
            "COALESCE": exp.Coalesce.from_arg_list,
            "NULLIF": exp.Nullif.from_arg_list,
            "IF": exp.If.from_arg_list,
            # JSON functions
            "JSON_VALUE": lambda args: exp.Anonymous(this="JSON_VALUE", expressions=args),
            "JSON_QUERY": lambda args: exp.Anonymous(this="JSON_QUERY", expressions=args),
            # Array functions
            "CARDINALITY": lambda args: exp.Anonymous(this="CARDINALITY", expressions=args),
            "ELEMENT": lambda args: exp.Anonymous(this="ELEMENT", expressions=args),
            # Type conversion
            "TRY_CAST": exp.TryCast.from_arg_list,
        }

    class Generator(Generator):
        """Flink SQL generator with custom type mappings."""

        # Map sqlglot types to Flink SQL types
        TYPE_MAPPING = {
            exp.DataType.Type.VARCHAR: "STRING",
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.CHAR: "STRING",
            exp.DataType.Type.NVARCHAR: "STRING",
            exp.DataType.Type.NCHAR: "STRING",
            exp.DataType.Type.VARBINARY: "BYTES",
            exp.DataType.Type.BINARY: "BYTES",
            exp.DataType.Type.TINYINT: "TINYINT",
            exp.DataType.Type.SMALLINT: "SMALLINT",
            exp.DataType.Type.INT: "INT",
            exp.DataType.Type.BIGINT: "BIGINT",
            exp.DataType.Type.FLOAT: "FLOAT",
            exp.DataType.Type.DOUBLE: "DOUBLE",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.BOOLEAN: "BOOLEAN",
            exp.DataType.Type.DATE: "DATE",
            exp.DataType.Type.TIME: "TIME",
            exp.DataType.Type.DATETIME: "TIMESTAMP(3)",
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP(3)",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP_LTZ(3)",
        }


# Flink-specific type inference helpers
FLINK_WINDOW_FUNCTIONS = {
    "TUMBLE_START": "TIMESTAMP(3)",
    "TUMBLE_END": "TIMESTAMP(3)",
    "TUMBLE_ROWTIME": "TIMESTAMP(3)",
    "TUMBLE_PROCTIME": "TIMESTAMP(3)",
    "HOP_START": "TIMESTAMP(3)",
    "HOP_END": "TIMESTAMP(3)",
    "HOP_ROWTIME": "TIMESTAMP(3)",
    "HOP_PROCTIME": "TIMESTAMP(3)",
    "SESSION_START": "TIMESTAMP(3)",
    "SESSION_END": "TIMESTAMP(3)",
    "SESSION_ROWTIME": "TIMESTAMP(3)",
    "SESSION_PROCTIME": "TIMESTAMP(3)",
    "PROCTIME": "TIMESTAMP(3)",
}


def get_flink_function_type(func_name: str) -> str | None:
    """Get the return type of a Flink-specific function.

    Args:
        func_name: The function name (case-insensitive)

    Returns:
        The Flink SQL type string, or None if not a known Flink function
    """
    return FLINK_WINDOW_FUNCTIONS.get(func_name.upper())


def is_flink_window_function(func_name: str) -> bool:
    """Check if a function is a Flink window function.

    Args:
        func_name: The function name (case-insensitive)

    Returns:
        True if it's a Flink window function
    """
    upper_name = func_name.upper()
    return (
        upper_name.startswith("TUMBLE") or
        upper_name.startswith("HOP") or
        upper_name.startswith("SESSION") or
        upper_name in ("PROCTIME", "ROWTIME")
    )
