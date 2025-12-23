"""Flink SQL dialect for sqlglot.

This module defines a custom Flink SQL dialect for sqlglot to properly parse
and handle Flink-specific SQL patterns including:
- TUMBLE, HOP, SESSION windowing functions
- TUMBLE_START, TUMBLE_END, etc. time attribute functions
- PROCTIME() for processing time
- Flink-specific data types
- MATCH_RECOGNIZE for Complex Event Processing (CEP)
- FOR SYSTEM_TIME AS OF for temporal/versioned table joins
- TABLE() TVF with CUMULATE, TUMBLE, HOP windowing functions
"""

from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


class FlinkDialect(Dialect):
    """Custom Flink SQL dialect for sqlglot.

    This dialect extends sqlglot's default dialect to support Flink SQL syntax,
    including windowing functions, MATCH_RECOGNIZE for CEP, temporal joins,
    and Flink-specific types.
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
            # MATCH_RECOGNIZE for Complex Event Processing
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            # Temporal join support (FOR SYSTEM_TIME AS OF)
            "FOR SYSTEM_TIME": TokenType.TIMESTAMP_SNAPSHOT,
            "FOR SYSTEM TIME": TokenType.TIMESTAMP_SNAPSHOT,
        }

    class Parser(Parser):
        """Flink SQL parser with custom function and syntax support."""

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
            # Flink TVF windowing functions
            "CUMULATE": lambda args: exp.Anonymous(this="CUMULATE", expressions=args),
            # DESCRIPTOR for column references in TVFs
            "DESCRIPTOR": lambda args: exp.Anonymous(this="DESCRIPTOR", expressions=args),
        }

        def _parse_lambda(self, alias: bool = False) -> t.Optional[exp.Expression]:
            """Override to handle TABLE keyword inside function arguments.

            In Flink SQL, windowing TVFs use syntax like:
                TUMBLE(TABLE orders, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE)

            The TABLE keyword before a table name needs special handling.
            """
            # Check for TABLE keyword (Flink TVF syntax)
            if self._match(TokenType.TABLE):
                # Parse the table reference after TABLE keyword
                table = self._parse_table_parts()
                if table:
                    # Wrap in Anonymous to preserve the TABLE keyword context
                    return exp.Anonymous(this="TABLE", expressions=[table])
                else:
                    self._retreat(self._index - 1)

            # Fall back to parent implementation
            return super()._parse_lambda(alias=alias)

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
    # TVF window output columns
    "window_start": "TIMESTAMP(3)",
    "window_end": "TIMESTAMP(3)",
    "window_time": "TIMESTAMP(3)",
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
        upper_name in ("PROCTIME", "ROWTIME", "CUMULATE")
    )
