"""Sandboxed AST-based formula parsing and evaluation engine for payloaded.

Zero external dependencies. Securely parses and evaluates expressions containing
column references, string transformations, concatenation, slicing, and stateful
generators (e.g. sequence counters, timestamps) without using `eval()`.
"""

from __future__ import annotations

import ast
from datetime import datetime, timezone
import re
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import uuid


class FormulaError(Exception):
    """Base exception for formula parsing or evaluation errors."""


class FormulaSecurityError(FormulaError):
    """Raised when an expression attempts to use unauthorized syntax or access private attributes."""


class FormulaEvaluationError(FormulaError):
    """Raised when an expression fails during evaluation against row data."""


# ---------------------------------------------------------------------------
# Standard Function Registry
# ---------------------------------------------------------------------------

def _fn_upper(val: Any) -> str:
    return "" if val is None else str(val).upper()


def _fn_lower(val: Any) -> str:
    return "" if val is None else str(val).lower()


def _fn_strip(val: Any) -> str:
    return "" if val is None else str(val).strip()


def _fn_lstrip(val: Any) -> str:
    return "" if val is None else str(val).lstrip()


def _fn_rstrip(val: Any) -> str:
    return "" if val is None else str(val).rstrip()


def _fn_replace(val: Any, old: Any, new: Any) -> str:
    s = "" if val is None else str(val)
    return s.replace(str(old), str(new))


def _fn_lpad(val: Any, length: int, char: str = "0") -> str:
    s = "" if val is None else str(val)
    return s.rjust(int(length), str(char))


def _fn_rpad(val: Any, length: int, char: str = " ") -> str:
    s = "" if val is None else str(val)
    return s.ljust(int(length), str(char))


def _fn_slice(val: Any, start: Optional[int] = None, end: Optional[int] = None) -> str:
    s = "" if val is None else str(val)
    st = None if start is None else int(start)
    en = None if end is None else int(end)
    return s[st:en]


def _fn_len(val: Any) -> int:
    if val is None:
        return 0
    return len(str(val))


def _fn_coalesce(*args: Any) -> Any:
    for a in args:
        if a is not None and str(a).strip() != "":
            return a
    return ""


def _fn_int(val: Any) -> int:
    if val is None or str(val).strip() == "":
        return 0
    return int(float(str(val).strip()))


def _fn_float(val: Any) -> float:
    if val is None or str(val).strip() == "":
        return 0.0
    return float(str(val).strip())


def _fn_str(val: Any) -> str:
    return "" if val is None else str(val)


def _fn_round(val: Any, digits: int = 0) -> float:
    if val is None or str(val).strip() == "":
        return 0.0
    return round(float(str(val).strip()), int(digits))


def _fn_now(format: str = "%Y-%m-%dT%H:%M:%SZ") -> str:
    return datetime.now(timezone.utc).strftime(format)


def _fn_uuid() -> str:
    return str(uuid.uuid4())


def _fn_date_format(val: Any, in_format: str, out_format: str) -> str:
    if val is None or str(val).strip() == "":
        return ""
    dt = datetime.strptime(str(val).strip(), in_format)
    return dt.strftime(out_format)


BASE_FUNCTIONS: Dict[str, Callable[..., Any]] = {
    "upper": _fn_upper,
    "lower": _fn_lower,
    "strip": _fn_strip,
    "trim": _fn_strip,
    "lstrip": _fn_lstrip,
    "rstrip": _fn_rstrip,
    "replace": _fn_replace,
    "lpad": _fn_lpad,
    "rpad": _fn_rpad,
    "slice": _fn_slice,
    "len": _fn_len,
    "coalesce": _fn_coalesce,
    "int": _fn_int,
    "float": _fn_float,
    "str": _fn_str,
    "round": _fn_round,
    "now": _fn_now,
    "uuid": _fn_uuid,
    "date_format": _fn_date_format,
}


# ---------------------------------------------------------------------------
# Generator Context
# ---------------------------------------------------------------------------

class GeneratorContext:
    """Tracks stateful generator sequence counters across scopes.

    Scopes:
    - 'parent': Resets whenever the parent entity changes.
    - 'payload': Resets on every new payload generated.
    - 'global': Never resets throughout the entire execution.
    """

    def __init__(self) -> None:
        # Maps generator key -> counter value
        self.global_counters: Dict[str, int] = {}
        self.payload_counters: Dict[Tuple[int, str], int] = {}
        self.parent_counters: Dict[Tuple[str, str], int] = {}

    def next_sequence(
        self,
        gen_id: str,
        start: int = 1,
        step: int = 1,
        scope: str = "parent",
        parent_id: Optional[str] = None,
        payload_index: int = 0,
    ) -> int:
        """Increment and return the next number for the specified sequence."""
        norm_scope = scope.lower().strip()
        if norm_scope == "global":
            if gen_id not in self.global_counters:
                self.global_counters[gen_id] = start
            val = self.global_counters[gen_id]
            self.global_counters[gen_id] += step
            return val

        elif norm_scope == "payload":
            key = (payload_index, gen_id)
            if key not in self.payload_counters:
                self.payload_counters[key] = start
            val = self.payload_counters[key]
            self.payload_counters[key] += step
            return val

        else:
            # scope == "parent"
            p_id = parent_id or "default_parent"
            p_key = (p_id, gen_id)
            if p_key not in self.parent_counters:
                self.parent_counters[p_key] = start
            val = self.parent_counters[p_key]
            self.parent_counters[p_key] += step
            return val


# ---------------------------------------------------------------------------
# AST Security Visitor & Evaluator
# ---------------------------------------------------------------------------

ALLOWED_NODE_TYPES = (
    ast.Expression,
    ast.BinOp,
    ast.UnaryOp,
    ast.Call,
    ast.Constant,
    ast.Name,
    ast.Subscript,
    ast.Slice,
    ast.keyword,
    ast.Load,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.BitAnd,  # for & operator
    ast.USub,
    ast.UAdd,
)


class SafeASTEvaluator:
    """Evaluates an AST node in a secure sandbox."""

    def __init__(
        self,
        tree: ast.AST,
        col_alias_to_col: Dict[str, str],
        gen_context: Optional[GeneratorContext] = None,
        parent_id: Optional[str] = None,
        payload_index: int = 0,
    ):
        self.tree = tree
        self.col_alias_to_col = col_alias_to_col
        self.gen_context = gen_context or GeneratorContext()
        self.parent_id = parent_id
        self.payload_index = payload_index

    def evaluate(self, row_dict: Dict[Any, Any]) -> Any:
        """Evaluate tree against a dictionary representing the current row."""
        return self._eval_node(self.tree, row_dict)

    def _eval_node(self, node: ast.AST, row: Dict[Any, Any]) -> Any:
        if isinstance(node, ast.Expression):
            return self._eval_node(node.body, row)

        elif isinstance(node, ast.Constant):
            return node.value

        elif isinstance(node, ast.Name):
            var_name = node.id
            # 1. Check if it's a column alias
            if var_name in self.col_alias_to_col:
                orig_col = self.col_alias_to_col[var_name]
                # Look up in row dictionary
                if orig_col in row:
                    val = row[orig_col]
                else:
                    # Case-insensitive / whitespace-tolerant lookup in row keys
                    val = None
                    for k, v in row.items():
                        if str(k).strip() == str(orig_col).strip():
                            val = v
                            break
                return "" if val is None else val

            # 2. Check if it's a registered function name
            if var_name in BASE_FUNCTIONS or var_name == "sequence":
                return var_name

            # Unrecognized identifier
            return ""

        elif isinstance(node, ast.BinOp):
            left_val = self._eval_node(node.left, row)
            right_val = self._eval_node(node.right, row)

            # Handle & as string concatenation (Excel/PowerQuery style)
            if isinstance(node.op, (ast.BitAnd,)):
                return str("" if left_val is None else left_val) + str("" if right_val is None else right_val)

            # Handle +: string concatenation if either side is str
            if isinstance(node.op, ast.Add):
                if isinstance(left_val, str) or isinstance(right_val, str):
                    return str("" if left_val is None else left_val) + str("" if right_val is None else right_val)
                try:
                    return left_val + right_val
                except Exception:
                    return str("" if left_val is None else left_val) + str("" if right_val is None else right_val)

            elif isinstance(node.op, ast.Sub):
                return (left_val or 0) - (right_val or 0)
            elif isinstance(node.op, ast.Mult):
                return (left_val or 0) * (right_val or 0)
            elif isinstance(node.op, ast.Div):
                return (left_val or 0) / (right_val or 1)
            elif isinstance(node.op, ast.FloorDiv):
                return (left_val or 0) // (right_val or 1)
            elif isinstance(node.op, ast.Mod):
                return (left_val or 0) % (right_val or 1)
            elif isinstance(node.op, ast.Pow):
                return (left_val or 0) ** (right_val or 1)
            else:
                raise FormulaEvaluationError(f"Unsupported binary operator: {type(node.op)}")

        elif isinstance(node, ast.UnaryOp):
            operand_val = self._eval_node(node.operand, row)
            if isinstance(node.op, ast.USub):
                return -operand_val
            elif isinstance(node.op, ast.UAdd):
                return +operand_val
            else:
                raise FormulaEvaluationError(f"Unsupported unary operator: {type(node.op)}")

        elif isinstance(node, ast.Subscript):
            target = self._eval_node(node.value, row)
            target_str = "" if target is None else str(target)
            slice_node = node.slice
            if isinstance(slice_node, ast.Slice):
                lower = self._eval_node(slice_node.lower, row) if slice_node.lower else None
                upper = self._eval_node(slice_node.upper, row) if slice_node.upper else None
                step = self._eval_node(slice_node.step, row) if slice_node.step else None
                return target_str[lower:upper:step]
            else:
                idx = self._eval_node(slice_node, row)
                try:
                    return target_str[int(idx)]
                except IndexError:
                    return ""

        elif isinstance(node, ast.Call):
            func_name = self._eval_node(node.func, row)
            args = [self._eval_node(arg, row) for arg in node.args]
            kwargs = {kw.arg: self._eval_node(kw.value, row) for kw in node.keywords if kw.arg is not None}

            if func_name == "sequence":
                # Generator sequence
                start = kwargs.get("start", args[0] if len(args) > 0 else 1)
                step = kwargs.get("step", args[1] if len(args) > 1 else 1)
                scope = kwargs.get("scope", args[2] if len(args) > 2 else "parent")
                gen_id = kwargs.get("id", str(node.lineno if hasattr(node, "lineno") else "seq"))
                return self.gen_context.next_sequence(
                    gen_id=str(gen_id),
                    start=int(start),
                    step=int(step),
                    scope=str(scope),
                    parent_id=self.parent_id,
                    payload_index=self.payload_index,
                )

            if func_name in BASE_FUNCTIONS:
                try:
                    return BASE_FUNCTIONS[func_name](*args, **kwargs)
                except Exception as exc:
                    raise FormulaEvaluationError(f"Error evaluating function '{func_name}': {exc}") from exc

            raise FormulaEvaluationError(f"Unknown or unauthorized function: {func_name}")

        else:
            raise FormulaSecurityError(f"Disallowed AST node: {type(node).__name__}")


# ---------------------------------------------------------------------------
# Compiled Formula
# ---------------------------------------------------------------------------

class CompiledFormula:
    """Pre-compiled formula object ready for evaluation against rows."""

    def __init__(self, raw_expression: str):
        self.raw_expression = raw_expression.strip()
        self.referenced_columns: List[str] = []
        self.col_alias_to_col: Dict[str, str] = {}
        self.tree = self._compile(self.raw_expression)

    def _compile(self, expr: str) -> ast.AST:
        """Parse expression into a secure, validated AST."""
        # 1. Extract {column_name} references and replace with safe identifiers
        # E.g. {Batch Number} -> __col_0__
        col_pattern = re.compile(r"\{([^{}]+)\}")
        matches = col_pattern.findall(expr)

        normalized_expr = expr
        alias_map = {}
        for idx, col_name in enumerate(matches):
            col_clean = col_name.strip()
            alias = f"__col_{idx}__"
            alias_map[alias] = col_clean
            self.referenced_columns.append(col_clean)
            # Replace {col_name} with alias
            normalized_expr = normalized_expr.replace(f"{{{col_name}}}", alias)

        self.col_alias_to_col = alias_map

        # 2. Parse using Python's ast.parse in 'eval' mode
        try:
            tree = ast.parse(normalized_expr, mode="eval")
        except SyntaxError as e:
            # If the syntax error was caused by a statement keyword, raise FormulaSecurityError
            statement_keywords = ("import ", "def ", "class ", "return ", "lambda ", "del ", "yield ")
            if any(re.search(rf"\b{re.escape(kw.strip())}\b", normalized_expr) for kw in statement_keywords):
                raise FormulaSecurityError(
                    f"Statements like import/def/class are strictly forbidden in formula '{self.raw_expression}'."
                ) from e
            raise FormulaError(f"Syntax error in formula '{self.raw_expression}': {e}") from e

        # 3. Security validation: walk AST and ensure NO disallowed nodes exist
        for node in ast.walk(tree):
            if not isinstance(node, ALLOWED_NODE_TYPES):
                raise FormulaSecurityError(
                    f"Disallowed syntax '{type(node).__name__}' in formula '{self.raw_expression}'. "
                    "Only safe arithmetic, string operations, slicing, and whitelisted functions are permitted."
                )
            # Ensure no private attribute access
            if isinstance(node, ast.Attribute):
                raise FormulaSecurityError(
                    f"Attribute access (e.g. '.{node.attr}') is strictly forbidden in formulas."
                )

        return tree

    def evaluate(
        self,
        row: Dict[Any, Any],
        gen_context: Optional[GeneratorContext] = None,
        parent_id: Optional[str] = None,
        payload_index: int = 0,
    ) -> Any:
        """Evaluate the pre-compiled formula against a row dictionary."""
        evaluator = SafeASTEvaluator(
            tree=self.tree,
            col_alias_to_col=self.col_alias_to_col,
            gen_context=gen_context,
            parent_id=parent_id,
            payload_index=payload_index,
        )
        return evaluator.evaluate(row)
