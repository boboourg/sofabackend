"""JSON schema inference primitives."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NodeSummary:
    """Recursive description of an observed JSON node."""

    path: str
    occurrence_count: int = 0
    kind_counts: dict[str, int] = field(default_factory=dict)
    examples: list[str] = field(default_factory=list)
    children: dict[str, "NodeSummary"] = field(default_factory=dict)
    item_summary: "NodeSummary | None" = None
    object_instance_count: int = 0
    array_instance_count: int = 0
    null_count: int = 0
    min_items: int | None = None
    max_items: int = 0
    total_items: int = 0

    def observe(self, value: Any) -> None:
        self.occurrence_count += 1
        kind = detect_kind(value)
        self.kind_counts[kind] = self.kind_counts.get(kind, 0) + 1
        self._remember_example(value)

        if kind == "object":
            self.object_instance_count += 1
            assert isinstance(value, dict)
            for key, child_value in value.items():
                child = self.children.setdefault(key, NodeSummary(path=f"{self.path}.{key}"))
                child.observe(child_value)
            return

        if kind == "array":
            self.array_instance_count += 1
            assert isinstance(value, list)
            length = len(value)
            self.total_items += length
            self.min_items = length if self.min_items is None else min(self.min_items, length)
            self.max_items = max(self.max_items, length)
            if self.item_summary is None:
                self.item_summary = NodeSummary(path=f"{self.path}[]")
            for item in value:
                self.item_summary.observe(item)
            return

        if kind == "null":
            self.null_count += 1

    def rendered_types(self) -> str:
        kinds = sorted(self.kind_counts)
        if kinds == ["array"] and self.item_summary is not None:
            return f"array<{self.item_summary.rendered_types()}>"
        if kinds == ["object"]:
            return "object"
        return " | ".join(kinds)

    def is_required_for_parent(self, parent_object_count: int) -> bool:
        return parent_object_count > 0 and self.occurrence_count == parent_object_count

    def is_nullable(self) -> bool:
        return "null" in self.kind_counts

    def candidate_keys(self) -> list[str]:
        if not self.children:
            return []
        candidates = []
        for key in self.children:
            lowered = key.lower()
            if lowered in {"id", "uuid", "slug", "code"}:
                candidates.append(key)
            elif lowered.endswith("id") or lowered.endswith("_id"):
                candidates.append(key)
        preferred_order = {"id": 0, "uuid": 1, "slug": 2, "code": 3}
        return sorted(candidates, key=lambda item: (preferred_order.get(item.lower(), 99), item.lower()))

    def suggested_table_name(self) -> str:
        segments = [segment.replace("[]", "") for segment in self.path.split(".") if segment and segment != "root"]
        if not segments:
            return "response_root"
        normalized = [_singularize(_slugify(segment)) for segment in segments]
        return "_".join(part for part in normalized if part)

    def field_rows(self) -> list[dict[str, str]]:
        rows = []
        for field_name in sorted(self.children):
            child = self.children[field_name]
            rows.append(
                {
                    "field": field_name,
                    "types": child.rendered_types(),
                    "required": "yes" if child.is_required_for_parent(self.object_instance_count) else "no",
                    "nullable": "yes" if child.is_nullable() else "no",
                    "example": child.examples[0] if child.examples else "",
                    "notes": describe_child(child),
                }
            )
        return rows

    def collect_object_nodes(self) -> list["NodeSummary"]:
        nodes = []
        if self.object_instance_count:
            nodes.append(self)
        for child in self.children.values():
            nodes.extend(child.collect_object_nodes())
        if self.item_summary is not None:
            nodes.extend(self.item_summary.collect_object_nodes())
        return nodes

    def collect_array_nodes(self) -> list["NodeSummary"]:
        nodes = []
        if self.array_instance_count:
            nodes.append(self)
        for child in self.children.values():
            nodes.extend(child.collect_array_nodes())
        if self.item_summary is not None:
            nodes.extend(self.item_summary.collect_array_nodes())
        return nodes

    def _remember_example(self, value: Any) -> None:
        if len(self.examples) >= 3:
            return
        rendered = render_example(value)
        if rendered not in self.examples:
            self.examples.append(rendered)


def infer_schema(payload: Any) -> NodeSummary:
    """Infer a recursive schema summary from a JSON payload."""

    root = NodeSummary(path="root")
    root.observe(payload)
    return root


def detect_kind(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return type(value).__name__.lower()


def render_example(value: Any, limit: int = 80) -> str:
    if isinstance(value, dict):
        preview = "{...}"
    elif isinstance(value, list):
        preview = f"[{len(value)} items]"
    else:
        preview = repr(value)
    return preview[:limit]


def describe_child(child: NodeSummary) -> str:
    notes = []
    if child.object_instance_count:
        notes.append(f"object path `{child.path}`")
    if child.array_instance_count:
        count_note = f"{child.min_items or 0}..{child.max_items} items"
        notes.append(f"array observed, {count_note}")
        if child.item_summary is not None and child.item_summary.object_instance_count:
            notes.append(f"suggested child table `{child.item_summary.suggested_table_name()}`")
    if child.candidate_keys():
        notes.append(f"candidate keys: {', '.join(child.candidate_keys())}")
    return "; ".join(notes)


def _slugify(value: str) -> str:
    chars = []
    previous_was_separator = False
    for char in value.lower():
        if char.isalnum():
            chars.append(char)
            previous_was_separator = False
        elif not previous_was_separator:
            chars.append("_")
            previous_was_separator = True
    return "".join(chars).strip("_")


def _singularize(value: str) -> str:
    if value.endswith("ies") and len(value) > 3:
        return value[:-3] + "y"
    if value.endswith("ses") and len(value) > 3:
        return value[:-2]
    if value.endswith("s") and not value.endswith("ss") and len(value) > 1:
        return value[:-1]
    return value
