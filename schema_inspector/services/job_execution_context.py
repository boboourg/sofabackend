"""Per-job execution context shared across worker handlers and audit loggers."""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass


@dataclass(frozen=True)
class JobExecutionContext:
    job_run_id: str
    job_id: str
    job_type: str | None
    trace_id: str | None
    worker_id: str


_CURRENT_JOB_EXECUTION_CONTEXT: ContextVar[JobExecutionContext | None] = ContextVar(
    "schema_inspector_job_execution_context",
    default=None,
)


def current_job_execution_context() -> JobExecutionContext | None:
    return _CURRENT_JOB_EXECUTION_CONTEXT.get()


def push_job_execution_context(context: JobExecutionContext) -> Token[JobExecutionContext | None]:
    return _CURRENT_JOB_EXECUTION_CONTEXT.set(context)


def reset_job_execution_context(token: Token[JobExecutionContext | None]) -> None:
    _CURRENT_JOB_EXECUTION_CONTEXT.reset(token)
