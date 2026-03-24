from __future__ import annotations

from backtestforecast.schemas.common import RemediationActionResponse, RemediationActionsResponse

_ACTIVE_STATUSES = frozenset({"queued", "running"})


def build_job_remediation_actions(
    *,
    resource_type: str,
    resource_id: str,
    status: str,
    base_path: str,
    retry_path: str | None = None,
) -> RemediationActionsResponse:
    active = status in _ACTIVE_STATUSES
    terminal = not active
    actions: list[RemediationActionResponse] = [
        RemediationActionResponse(
            action="cancel",
            label="Cancel Job",
            description="Stop this queued or running job safely.",
            method="POST",
            href=f"{base_path}/cancel",
            allowed=active,
            reason=None if active else "Only queued or running jobs can be cancelled.",
        ),
        RemediationActionResponse(
            action="delete",
            label="Delete Job",
            description="Delete this job after it reaches a terminal state.",
            method="DELETE",
            href=base_path,
            allowed=terminal,
            reason=None if terminal else "Cancel the job first, then delete it after it becomes terminal.",
        ),
    ]
    if retry_path is not None:
        retry_allowed = status == "failed"
        actions.append(
            RemediationActionResponse(
                action="retry",
                label="Retry Job",
                description="Create a new retry job from this failed request.",
                method="POST",
                href=retry_path,
                allowed=retry_allowed,
                reason=None if retry_allowed else "Only failed jobs can be retried.",
            )
        )
    return RemediationActionsResponse(
        resource_type=resource_type,
        resource_id=resource_id,
        status=status,
        actions=actions,
    )
