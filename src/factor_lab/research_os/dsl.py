"""Typed, point-in-time-safe factor expression graphs.

The research OS deliberately does not execute arbitrary Python expressions.
Factors are represented as a small typed DAG whose dependencies, temporal
lags and availability can be audited before any values are computed.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence, Union

import numpy as np
import pandas as pd

from .field_safety import is_forward_derived_field


class ValueType(str, Enum):
    NUMERIC = "numeric"
    BOOLEAN = "boolean"
    CATEGORICAL = "categorical"


class FieldRole(str, Enum):
    FEATURE = "feature"
    IDENTIFIER = "identifier"
    LABEL = "label"


class Availability(str, Enum):
    PRE_OPEN = "pre_open"
    OPEN = "open"
    INTRADAY = "intraday"
    CLOSE = "close"
    POST_CLOSE = "post_close"
    NEXT_SESSION = "next_session"


class DecisionPoint(str, Enum):
    PRE_OPEN = "pre_open"
    AFTER_OPEN = "after_open"
    AFTER_CLOSE = "after_close"


class UnaryOperation(str, Enum):
    NEGATE = "negate"
    ABS = "abs"
    LOG = "log"
    SQRT = "sqrt"
    NOT = "not"


class BinaryOperation(str, Enum):
    ADD = "add"
    SUBTRACT = "subtract"
    MULTIPLY = "multiply"
    DIVIDE = "divide"
    GREATER = "greater"
    GREATER_EQUAL = "greater_equal"
    LESS = "less"
    LESS_EQUAL = "less_equal"
    EQUAL = "equal"
    AND = "and"
    OR = "or"


class RollingOperation(str, Enum):
    MEAN = "rolling_mean"
    STD = "rolling_std"
    SUM = "rolling_sum"
    MIN = "rolling_min"
    MAX = "rolling_max"


class CrossSectionOperation(str, Enum):
    RANK = "rank"
    ZSCORE = "zscore"
    WINSORIZE = "winsorize"


@dataclass(frozen=True)
class FieldSpec:
    name: str
    value_type: ValueType = ValueType.NUMERIC
    role: FieldRole = FieldRole.FEATURE
    availability: Availability = Availability.CLOSE
    minimum_lag_sessions: int = 0
    available_at_column: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.value_type, ValueType):
            object.__setattr__(self, "value_type", ValueType(str(self.value_type)))
        if not isinstance(self.role, FieldRole):
            object.__setattr__(self, "role", FieldRole(str(self.role)))
        if not isinstance(self.availability, Availability):
            object.__setattr__(self, "availability", Availability(str(self.availability)))
        if not self.name:
            raise ValueError("field name must not be empty")
        if self.minimum_lag_sessions < 0:
            raise ValueError("minimum_lag_sessions must be non-negative")


@dataclass(frozen=True)
class FieldNode:
    node_id: str
    field: str


@dataclass(frozen=True)
class ConstantNode:
    node_id: str
    value: float | bool


@dataclass(frozen=True)
class LagNode:
    node_id: str
    input_id: str
    periods: int


@dataclass(frozen=True)
class RollingNode:
    node_id: str
    input_id: str
    operation: RollingOperation
    window: int
    min_periods: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.operation, RollingOperation):
            object.__setattr__(self, "operation", RollingOperation(str(self.operation)))


@dataclass(frozen=True)
class UnaryNode:
    node_id: str
    input_id: str
    operation: UnaryOperation

    def __post_init__(self) -> None:
        if not isinstance(self.operation, UnaryOperation):
            object.__setattr__(self, "operation", UnaryOperation(str(self.operation)))


@dataclass(frozen=True)
class BinaryNode:
    node_id: str
    left_id: str
    right_id: str
    operation: BinaryOperation

    def __post_init__(self) -> None:
        if not isinstance(self.operation, BinaryOperation):
            object.__setattr__(self, "operation", BinaryOperation(str(self.operation)))


@dataclass(frozen=True)
class CrossSectionNode:
    node_id: str
    input_id: str
    operation: CrossSectionOperation
    lower_quantile: float = 0.01
    upper_quantile: float = 0.99

    def __post_init__(self) -> None:
        if not isinstance(self.operation, CrossSectionOperation):
            object.__setattr__(self, "operation", CrossSectionOperation(str(self.operation)))


@dataclass(frozen=True)
class ConditionalNode:
    node_id: str
    condition_id: str
    true_id: str
    false_id: str


@dataclass(frozen=True)
class NeutralizeNode:
    node_id: str
    input_id: str
    exposure_ids: tuple[str, ...]
    categorical_exposure_ids: tuple[str, ...] = ()


FactorNode = Union[
    FieldNode,
    ConstantNode,
    LagNode,
    RollingNode,
    UnaryNode,
    BinaryNode,
    CrossSectionNode,
    ConditionalNode,
    NeutralizeNode,
]


@dataclass(frozen=True)
class FactorGraph:
    nodes: tuple[FactorNode, ...]
    output_id: str
    schema_version: str = "research-os/factor-dsl/v1"

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "FactorGraph":
        if payload.get("schema_version", "research-os/factor-dsl/v1") != "research-os/factor-dsl/v1":
            raise ValueError("unsupported factor DSL schema version")
        nodes = tuple(_node_from_dict(row) for row in payload.get("nodes", ()))
        output_id = str(payload.get("output_id") or payload.get("output") or "")
        return cls(nodes=nodes, output_id=output_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "output_id": self.output_id,
            "nodes": [_node_to_dict(node) for node in self.nodes],
        }


@dataclass(frozen=True)
class EvaluationContext:
    entity_column: str = "ticker"
    time_column: str = "date"
    decision_point: DecisionPoint = DecisionPoint.AFTER_CLOSE
    decision_time_column: str | None = "decision_time"

    def __post_init__(self) -> None:
        if not isinstance(self.decision_point, DecisionPoint):
            object.__setattr__(self, "decision_point", DecisionPoint(str(self.decision_point)))


@dataclass(frozen=True)
class DslViolation:
    code: str
    node_id: str | None
    message: str


@dataclass(frozen=True)
class CompiledFactorGraph:
    graph: FactorGraph
    ordered_nodes: tuple[FactorNode, ...]
    output_type: ValueType
    field_lags: Mapping[str, int]
    field_lag_paths: Mapping[str, tuple[int, ...]]


class DslValidationError(ValueError):
    def __init__(self, violations: Sequence[DslViolation]):
        self.violations = tuple(violations)
        summary = "; ".join(f"{item.code}: {item.message}" for item in self.violations)
        super().__init__(summary or "invalid factor graph")


def factor_graph_from_spec(factor_spec: Mapping[str, Any] | Any) -> FactorGraph:
    """Load the typed graph carried by a Research OS ``FactorSpec``.

    Legacy string expressions are intentionally rejected: accepting them here
    would bypass the typed operation allowlist and point-in-time lineage audit.
    """

    expression = (
        factor_spec.get("expression")
        if isinstance(factor_spec, Mapping)
        else getattr(factor_spec, "expression", None)
    )
    if not isinstance(expression, Mapping):
        raise DslValidationError(
            (
                DslViolation(
                    "untyped_expression",
                    None,
                    "Research OS factors must use a typed factor-dsl/v1 mapping",
                ),
            )
        )
    return FactorGraph.from_dict(expression)


def _node_from_dict(row: Mapping[str, Any]) -> FactorNode:
    node_id = str(row.get("node_id") or row.get("id") or "")
    op = str(row.get("op") or "").lower()
    if op == "field":
        return FieldNode(node_id=node_id, field=str(row.get("field") or ""))
    if op == "constant":
        return ConstantNode(node_id=node_id, value=row.get("value"))
    if op == "lag":
        return LagNode(node_id=node_id, input_id=str(row.get("input_id") or row.get("input") or ""), periods=int(row.get("periods", 0)))
    if op in {item.value for item in RollingOperation}:
        return RollingNode(
            node_id=node_id,
            input_id=str(row.get("input_id") or row.get("input") or ""),
            operation=RollingOperation(op),
            window=int(row.get("window", 0)),
            min_periods=None if row.get("min_periods") is None else int(row["min_periods"]),
        )
    if op in {item.value for item in UnaryOperation}:
        return UnaryNode(node_id=node_id, input_id=str(row.get("input_id") or row.get("input") or ""), operation=UnaryOperation(op))
    if op in {item.value for item in BinaryOperation}:
        return BinaryNode(
            node_id=node_id,
            left_id=str(row.get("left_id") or row.get("left") or ""),
            right_id=str(row.get("right_id") or row.get("right") or ""),
            operation=BinaryOperation(op),
        )
    if op in {item.value for item in CrossSectionOperation}:
        return CrossSectionNode(
            node_id=node_id,
            input_id=str(row.get("input_id") or row.get("input") or ""),
            operation=CrossSectionOperation(op),
            lower_quantile=float(row.get("lower_quantile", 0.01)),
            upper_quantile=float(row.get("upper_quantile", 0.99)),
        )
    if op == "where":
        return ConditionalNode(
            node_id=node_id,
            condition_id=str(row.get("condition_id") or row.get("condition") or ""),
            true_id=str(row.get("true_id") or row.get("if_true") or ""),
            false_id=str(row.get("false_id") or row.get("if_false") or ""),
        )
    if op == "neutralize":
        return NeutralizeNode(
            node_id=node_id,
            input_id=str(row.get("input_id") or row.get("input") or ""),
            exposure_ids=tuple(str(item) for item in row.get("exposure_ids", row.get("exposures", ()))),
            categorical_exposure_ids=tuple(str(item) for item in row.get("categorical_exposure_ids", row.get("categorical_exposures", ()))),
        )
    raise ValueError(f"unsupported factor DSL operation: {op!r}")


def _node_to_dict(node: FactorNode) -> dict[str, Any]:
    if isinstance(node, FieldNode):
        return {"id": node.node_id, "op": "field", "field": node.field}
    if isinstance(node, ConstantNode):
        return {"id": node.node_id, "op": "constant", "value": node.value}
    if isinstance(node, LagNode):
        return {"id": node.node_id, "op": "lag", "input": node.input_id, "periods": node.periods}
    if isinstance(node, RollingNode):
        return {"id": node.node_id, "op": node.operation.value, "input": node.input_id, "window": node.window, "min_periods": node.min_periods}
    if isinstance(node, UnaryNode):
        return {"id": node.node_id, "op": node.operation.value, "input": node.input_id}
    if isinstance(node, BinaryNode):
        return {"id": node.node_id, "op": node.operation.value, "left": node.left_id, "right": node.right_id}
    if isinstance(node, CrossSectionNode):
        return {
            "id": node.node_id,
            "op": node.operation.value,
            "input": node.input_id,
            "lower_quantile": node.lower_quantile,
            "upper_quantile": node.upper_quantile,
        }
    if isinstance(node, ConditionalNode):
        return {"id": node.node_id, "op": "where", "condition": node.condition_id, "if_true": node.true_id, "if_false": node.false_id}
    return {
        "id": node.node_id,
        "op": "neutralize",
        "input": node.input_id,
        "exposures": list(node.exposure_ids),
        "categorical_exposures": list(node.categorical_exposure_ids),
    }


def _dependencies(node: FactorNode) -> tuple[str, ...]:
    if isinstance(node, (FieldNode, ConstantNode)):
        return ()
    if isinstance(node, (LagNode, RollingNode, UnaryNode, CrossSectionNode)):
        return (node.input_id,)
    if isinstance(node, BinaryNode):
        return (node.left_id, node.right_id)
    if isinstance(node, ConditionalNode):
        return (node.condition_id, node.true_id, node.false_id)
    return (node.input_id,) + node.exposure_ids


def _required_lag(spec: FieldSpec, decision_point: DecisionPoint) -> int:
    required = spec.minimum_lag_sessions
    available_same_session = {
        DecisionPoint.PRE_OPEN: {Availability.PRE_OPEN},
        DecisionPoint.AFTER_OPEN: {Availability.PRE_OPEN, Availability.OPEN},
        DecisionPoint.AFTER_CLOSE: {Availability.PRE_OPEN, Availability.OPEN, Availability.INTRADAY, Availability.CLOSE},
    }[decision_point]
    if spec.availability not in available_same_session:
        required = max(required, 1)
    return required


def compile_factor_graph(
    graph: FactorGraph | Mapping[str, Any],
    field_specs: Iterable[FieldSpec] | Mapping[str, FieldSpec],
    *,
    decision_point: DecisionPoint = DecisionPoint.AFTER_CLOSE,
) -> CompiledFactorGraph:
    """Validate topology, types and point-in-time lineage, then return a plan."""

    if not isinstance(graph, FactorGraph):
        graph = FactorGraph.from_dict(graph)
    specs = dict(field_specs) if isinstance(field_specs, Mapping) else {item.name: item for item in field_specs}
    node_map: dict[str, FactorNode] = {}
    violations: list[DslViolation] = []
    if graph.schema_version != "research-os/factor-dsl/v1":
        violations.append(DslViolation("unsupported_schema_version", None, graph.schema_version))
    for node in graph.nodes:
        if not node.node_id:
            violations.append(DslViolation("empty_node_id", None, "node id must not be empty"))
        elif node.node_id in node_map:
            violations.append(DslViolation("duplicate_node_id", node.node_id, "node id is duplicated"))
        else:
            node_map[node.node_id] = node
    if graph.output_id not in node_map:
        violations.append(DslViolation("missing_output", graph.output_id or None, "output node does not exist"))

    reachable: set[str] = set()

    def mark_reachable(node_id: str) -> None:
        if node_id in reachable or node_id not in node_map:
            return
        reachable.add(node_id)
        for dependency in _dependencies(node_map[node_id]):
            mark_reachable(dependency)

    mark_reachable(graph.output_id)
    for unused in sorted(set(node_map).difference(reachable)):
        violations.append(DslViolation("unused_node", unused, "node is not reachable from factor output"))

    ordered: list[FactorNode] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visited or node_id not in node_map:
            return
        if node_id in visiting:
            violations.append(DslViolation("cycle", node_id, "factor graph contains a dependency cycle"))
            return
        visiting.add(node_id)
        node = node_map[node_id]
        for dependency in _dependencies(node):
            if dependency not in node_map:
                violations.append(DslViolation("missing_dependency", node_id, f"dependency {dependency!r} does not exist"))
            else:
                visit(dependency)
        visiting.remove(node_id)
        visited.add(node_id)
        ordered.append(node)

    for node_id in node_map:
        visit(node_id)

    types: dict[str, ValueType] = {}
    lineages: dict[str, dict[str, set[int]]] = {}

    def merge_lineages(ids: Iterable[str]) -> dict[str, set[int]]:
        result: dict[str, set[int]] = {}
        for dependency in ids:
            for field, lags in lineages.get(dependency, {}).items():
                result.setdefault(field, set()).update(lags)
        return result

    numeric_binary = {BinaryOperation.ADD, BinaryOperation.SUBTRACT, BinaryOperation.MULTIPLY, BinaryOperation.DIVIDE}
    comparison_binary = {BinaryOperation.GREATER, BinaryOperation.GREATER_EQUAL, BinaryOperation.LESS, BinaryOperation.LESS_EQUAL, BinaryOperation.EQUAL}
    boolean_binary = {BinaryOperation.AND, BinaryOperation.OR}

    for node in ordered:
        node_id = node.node_id
        if isinstance(node, FieldNode):
            spec = specs.get(node.field)
            if spec is None:
                violations.append(DslViolation("unknown_field", node_id, f"field {node.field!r} is not registered"))
                continue
            if spec.role is not FieldRole.FEATURE:
                violations.append(DslViolation("forbidden_field_role", node_id, f"field {node.field!r} has role {spec.role.value}"))
            if is_forward_derived_field(
                node.field, strict_target_segments=True
            ):
                violations.append(DslViolation("forward_field", node_id, f"field {node.field!r} looks forward-derived"))
            types[node_id] = spec.value_type
            lineages[node_id] = {node.field: {0}}
        elif isinstance(node, ConstantNode):
            if not isinstance(node.value, (bool, int, float, np.number)):
                violations.append(DslViolation("invalid_constant", node_id, "constants must be numeric or boolean"))
                continue
            types[node_id] = ValueType.BOOLEAN if isinstance(node.value, (bool, np.bool_)) else ValueType.NUMERIC
            lineages[node_id] = {}
        elif isinstance(node, LagNode):
            if node.periods < 0:
                violations.append(DslViolation("negative_lag", node_id, "lead/negative lag is forbidden"))
            if node.input_id in types:
                types[node_id] = types[node.input_id]
            lineages[node_id] = {
                field: {lag + max(node.periods, 0) for lag in lags}
                for field, lags in lineages.get(node.input_id, {}).items()
            }
        elif isinstance(node, RollingNode):
            if node.window <= 0 or (node.min_periods is not None and not 1 <= node.min_periods <= node.window):
                violations.append(DslViolation("invalid_window", node_id, "rolling window/min_periods are invalid"))
            if types.get(node.input_id) not in {None, ValueType.NUMERIC}:
                violations.append(DslViolation("type_error", node_id, "rolling operations require a numeric input"))
            types[node_id] = ValueType.NUMERIC
            lineages[node_id] = merge_lineages((node.input_id,))
        elif isinstance(node, UnaryNode):
            expected = ValueType.BOOLEAN if node.operation is UnaryOperation.NOT else ValueType.NUMERIC
            if types.get(node.input_id) not in {None, expected}:
                violations.append(DslViolation("type_error", node_id, f"{node.operation.value} requires {expected.value}"))
            types[node_id] = expected
            lineages[node_id] = merge_lineages((node.input_id,))
        elif isinstance(node, BinaryNode):
            left_type, right_type = types.get(node.left_id), types.get(node.right_id)
            if node.operation in numeric_binary:
                if left_type not in {None, ValueType.NUMERIC} or right_type not in {None, ValueType.NUMERIC}:
                    violations.append(DslViolation("type_error", node_id, "arithmetic requires numeric inputs"))
                types[node_id] = ValueType.NUMERIC
            elif node.operation in boolean_binary:
                if left_type not in {None, ValueType.BOOLEAN} or right_type not in {None, ValueType.BOOLEAN}:
                    violations.append(DslViolation("type_error", node_id, "boolean operation requires boolean inputs"))
                types[node_id] = ValueType.BOOLEAN
            else:
                if node.operation is not BinaryOperation.EQUAL and (left_type not in {None, ValueType.NUMERIC} or right_type not in {None, ValueType.NUMERIC}):
                    violations.append(DslViolation("type_error", node_id, "ordered comparison requires numeric inputs"))
                types[node_id] = ValueType.BOOLEAN
            lineages[node_id] = merge_lineages((node.left_id, node.right_id))
        elif isinstance(node, CrossSectionNode):
            if types.get(node.input_id) not in {None, ValueType.NUMERIC}:
                violations.append(DslViolation("type_error", node_id, "cross-sectional transforms require numeric input"))
            if node.operation is CrossSectionOperation.WINSORIZE and not 0 <= node.lower_quantile < node.upper_quantile <= 1:
                violations.append(DslViolation("invalid_quantiles", node_id, "winsorize quantiles must satisfy 0 <= lower < upper <= 1"))
            types[node_id] = ValueType.NUMERIC
            lineages[node_id] = merge_lineages((node.input_id,))
        elif isinstance(node, ConditionalNode):
            if types.get(node.condition_id) not in {None, ValueType.BOOLEAN}:
                violations.append(DslViolation("type_error", node_id, "where condition must be boolean"))
            true_type, false_type = types.get(node.true_id), types.get(node.false_id)
            if true_type is not None and false_type is not None and true_type is not false_type:
                violations.append(DslViolation("type_error", node_id, "where branches must have the same type"))
            types[node_id] = true_type or false_type or ValueType.NUMERIC
            lineages[node_id] = merge_lineages((node.condition_id, node.true_id, node.false_id))
        else:
            all_inputs = (node.input_id,) + node.exposure_ids
            if types.get(node.input_id) not in {None, ValueType.NUMERIC}:
                violations.append(DslViolation("type_error", node_id, "neutralize target must be numeric"))
            for exposure in node.exposure_ids:
                if exposure in node.categorical_exposure_ids:
                    if types.get(exposure) not in {None, ValueType.CATEGORICAL}:
                        violations.append(DslViolation("type_error", node_id, f"categorical exposure {exposure!r} is not categorical"))
                elif types.get(exposure) not in {None, ValueType.NUMERIC}:
                    violations.append(DslViolation("type_error", node_id, f"exposure {exposure!r} is not numeric"))
            if not node.exposure_ids:
                violations.append(DslViolation("missing_exposure", node_id, "neutralize requires at least one exposure"))
            if not set(node.categorical_exposure_ids).issubset(node.exposure_ids):
                violations.append(DslViolation("invalid_categorical_exposure", node_id, "categorical exposures must be listed in exposure_ids"))
            types[node_id] = ValueType.NUMERIC
            lineages[node_id] = merge_lineages(all_inputs)

    output_type = types.get(graph.output_id, ValueType.NUMERIC)
    if graph.output_id in types and output_type is not ValueType.NUMERIC:
        violations.append(DslViolation("non_numeric_output", graph.output_id, "factor output must be numeric"))
    output_lineage = lineages.get(graph.output_id, {})
    for field, actual_lags in output_lineage.items():
        spec = specs.get(field)
        if spec is None:
            continue
        minimum = _required_lag(spec, decision_point)
        actual_lag = min(actual_lags)
        if actual_lag < minimum:
            violations.append(
                DslViolation(
                    "insufficient_lag",
                    graph.output_id,
                    f"field {field!r} requires lag {minimum}, observed {actual_lag}",
                )
            )
    if violations:
        raise DslValidationError(violations)
    return CompiledFactorGraph(
        graph=graph,
        ordered_nodes=tuple(ordered),
        output_type=output_type,
        field_lags={field: min(lags) for field, lags in output_lineage.items()},
        field_lag_paths={field: tuple(sorted(lags)) for field, lags in output_lineage.items()},
    )


def _rolling(series: pd.Series, entities: pd.Series, operation: RollingOperation, window: int, min_periods: int) -> pd.Series:
    grouped = series.groupby(entities, sort=False).rolling(window=window, min_periods=min_periods)
    if operation is RollingOperation.MEAN:
        result = grouped.mean()
    elif operation is RollingOperation.STD:
        result = grouped.std(ddof=0)
    elif operation is RollingOperation.SUM:
        result = grouped.sum()
    elif operation is RollingOperation.MIN:
        result = grouped.min()
    else:
        result = grouped.max()
    return result.reset_index(level=0, drop=True).sort_index()


def _neutralize(values: pd.Series, exposures: Sequence[pd.Series], categorical: Sequence[bool], times: pd.Series) -> pd.Series:
    output = pd.Series(np.nan, index=values.index, dtype=float)
    for _, index in times.groupby(times, sort=False).groups.items():
        y = pd.to_numeric(values.loc[index], errors="coerce")
        parts: list[pd.DataFrame] = []
        valid = y.notna()
        for exposure, is_categorical in zip(exposures, categorical):
            selected = exposure.loc[index]
            valid &= selected.notna()
            if is_categorical:
                encoded = pd.get_dummies(selected.astype("string"), drop_first=True, dtype=float)
                parts.append(encoded)
            else:
                parts.append(pd.DataFrame({"value": pd.to_numeric(selected, errors="coerce")}, index=index))
        valid_index = valid[valid].index
        if len(valid_index) < 2:
            continue
        matrices = [part.loc[valid_index].reset_index(drop=True) for part in parts]
        x_frame = pd.concat(matrices, axis=1) if matrices else pd.DataFrame(index=range(len(valid_index)))
        x = np.column_stack([np.ones(len(valid_index)), x_frame.to_numpy(dtype=float)])
        if not np.isfinite(x).all() or len(valid_index) <= x.shape[1]:
            continue
        y_values = y.loc[valid_index].to_numpy(dtype=float)
        coefficients, *_ = np.linalg.lstsq(x, y_values, rcond=None)
        output.loc[valid_index] = y_values - x @ coefficients
    return output


def evaluate_factor_graph(
    graph: FactorGraph | Mapping[str, Any],
    frame: pd.DataFrame,
    field_specs: Iterable[FieldSpec] | Mapping[str, FieldSpec],
    *,
    context: EvaluationContext = EvaluationContext(),
) -> pd.Series:
    """Evaluate a validated graph without mutating ``frame``.

    Rows are stably sorted by entity/time for temporal operations and returned
    in the caller's original order and index.  Precise vendor availability is
    checked against the decision timestamp after applying the graph's lag.
    """

    compiled = compile_factor_graph(graph, field_specs, decision_point=context.decision_point)
    specs = dict(field_specs) if isinstance(field_specs, Mapping) else {item.name: item for item in field_specs}
    required_columns = {context.entity_column, context.time_column}
    required_columns.update(compiled.field_lags)
    for field in compiled.field_lags:
        column = specs[field].available_at_column
        if column:
            required_columns.add(column)
            if context.decision_time_column:
                required_columns.add(context.decision_time_column)
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        raise KeyError(f"missing factor evaluation columns: {missing}")

    original_index = frame.index.copy()
    ordered = frame.loc[:, list(dict.fromkeys(required_columns))].copy()
    ordered["__factor_dsl_position__"] = np.arange(len(ordered))
    ordered = ordered.sort_values([context.entity_column, context.time_column, "__factor_dsl_position__"], kind="stable").reset_index(drop=True)
    entities = ordered[context.entity_column]
    times = ordered[context.time_column]

    for field, lag_paths in compiled.field_lag_paths.items():
        availability_column = specs[field].available_at_column
        if not availability_column:
            continue
        if not context.decision_time_column:
            raise DslValidationError((DslViolation("missing_decision_time", None, f"field {field!r} requires a decision timestamp"),))
        source_available = pd.to_datetime(ordered[availability_column], errors="coerce", utc=True)
        decisions = pd.to_datetime(ordered[context.decision_time_column], errors="coerce", utc=True)
        for lag in lag_paths:
            effective_available = source_available.groupby(entities, sort=False).shift(lag) if lag else source_available
            source_values = ordered[field].groupby(entities, sort=False).shift(lag) if lag else ordered[field]
            unavailable = source_values.notna() & (effective_available.isna() | decisions.isna() | (effective_available > decisions))
            if unavailable.any():
                raise DslValidationError(
                    (
                        DslViolation(
                            "not_available_at_decision",
                            None,
                            f"field {field!r} at lag {lag} has {int(unavailable.sum())} value(s) unavailable at decision time",
                        ),
                    )
                )

    values: dict[str, pd.Series] = {}
    for node in compiled.ordered_nodes:
        if isinstance(node, FieldNode):
            values[node.node_id] = ordered[node.field].copy()
        elif isinstance(node, ConstantNode):
            values[node.node_id] = pd.Series(node.value, index=ordered.index)
        elif isinstance(node, LagNode):
            values[node.node_id] = values[node.input_id].groupby(entities, sort=False).shift(node.periods)
        elif isinstance(node, RollingNode):
            source = pd.to_numeric(values[node.input_id], errors="coerce")
            values[node.node_id] = _rolling(source, entities, node.operation, node.window, node.min_periods or node.window)
        elif isinstance(node, UnaryNode):
            source = values[node.input_id]
            if node.operation is UnaryOperation.NEGATE:
                result = -pd.to_numeric(source, errors="coerce")
            elif node.operation is UnaryOperation.ABS:
                result = pd.to_numeric(source, errors="coerce").abs()
            elif node.operation is UnaryOperation.LOG:
                numeric = pd.to_numeric(source, errors="coerce")
                result = np.log(numeric.where(numeric > 0))
            elif node.operation is UnaryOperation.SQRT:
                numeric = pd.to_numeric(source, errors="coerce")
                result = np.sqrt(numeric.where(numeric >= 0))
            else:
                result = ~source.astype("boolean")
            values[node.node_id] = pd.Series(result, index=ordered.index)
        elif isinstance(node, BinaryNode):
            left, right = values[node.left_id], values[node.right_id]
            operation = node.operation
            if operation is BinaryOperation.ADD:
                result = left + right
            elif operation is BinaryOperation.SUBTRACT:
                result = left - right
            elif operation is BinaryOperation.MULTIPLY:
                result = left * right
            elif operation is BinaryOperation.DIVIDE:
                denominator = pd.to_numeric(right, errors="coerce")
                result = pd.to_numeric(left, errors="coerce") / denominator.where(denominator != 0)
            elif operation is BinaryOperation.GREATER:
                result = left > right
            elif operation is BinaryOperation.GREATER_EQUAL:
                result = left >= right
            elif operation is BinaryOperation.LESS:
                result = left < right
            elif operation is BinaryOperation.LESS_EQUAL:
                result = left <= right
            elif operation is BinaryOperation.EQUAL:
                result = left == right
            elif operation is BinaryOperation.AND:
                result = left.astype("boolean") & right.astype("boolean")
            else:
                result = left.astype("boolean") | right.astype("boolean")
            values[node.node_id] = pd.Series(result, index=ordered.index)
        elif isinstance(node, CrossSectionNode):
            source = pd.to_numeric(values[node.input_id], errors="coerce")
            grouped = source.groupby(times, sort=False)
            if node.operation is CrossSectionOperation.RANK:
                result = grouped.rank(method="average", pct=True)
            elif node.operation is CrossSectionOperation.ZSCORE:
                mean = grouped.transform("mean")
                standard_deviation = grouped.transform(lambda item: item.std(ddof=0))
                result = (source - mean) / standard_deviation.where(standard_deviation > 0)
            else:
                lower = grouped.transform(lambda item: item.quantile(node.lower_quantile))
                upper = grouped.transform(lambda item: item.quantile(node.upper_quantile))
                result = source.clip(lower=lower, upper=upper)
            values[node.node_id] = pd.Series(result, index=ordered.index)
        elif isinstance(node, ConditionalNode):
            condition = values[node.condition_id].astype("boolean").fillna(False)
            values[node.node_id] = pd.Series(np.where(condition, values[node.true_id], values[node.false_id]), index=ordered.index)
        else:
            exposures = [values[item] for item in node.exposure_ids]
            categorical = [item in node.categorical_exposure_ids for item in node.exposure_ids]
            values[node.node_id] = _neutralize(pd.to_numeric(values[node.input_id], errors="coerce"), exposures, categorical, times)

    output = pd.to_numeric(values[compiled.graph.output_id], errors="coerce")
    ordered_result = pd.DataFrame(
        {
            "position": ordered["__factor_dsl_position__"].to_numpy(),
            "value": output.to_numpy(dtype=float),
        }
    ).sort_values("position", kind="stable")
    return pd.Series(ordered_result["value"].to_numpy(), index=original_index, name=compiled.graph.output_id)
