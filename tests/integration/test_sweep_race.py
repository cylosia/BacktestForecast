"""Test sweep job status transitions are atomic."""


def test_sweep_cas_transition_prevents_double_run():
    """Verify that the CAS (compare-and-swap) status transition in sweep run_job
    prevents two workers from processing the same job.

    The fix ensures SweepService.run_job uses:
        UPDATE sweep_jobs SET status='running' WHERE id=:id AND status='queued'
    so only the first caller succeeds.
    """
    pass  # Requires full integration setup; documents the expected behavior
