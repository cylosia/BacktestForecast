from __future__ import annotations

import inspect
from pathlib import Path


def test_genetic_optimizer_supports_process_pool_serializable_evaluators() -> None:
    from backtestforecast.sweeps.genetic import GeneticOptimizer

    source = inspect.getsource(GeneticOptimizer._evaluate_population)
    assert 'ProcessPoolExecutor' in source
    assert 'SerializableFitnessEvaluator' in source


def test_sweep_service_builds_serializable_fitness_evaluator() -> None:
    source = Path('src/backtestforecast/services/sweeps.py').read_text()
    assert 'SerializableFitnessEvaluator' in source
    assert 'sweep_genetic_runtime' in source


def test_billing_service_is_split_across_component_classes() -> None:
    source = Path('src/backtestforecast/services/billing.py').read_text()
    assert 'CheckoutService' in source
    assert 'PortalService' in source
    assert 'WebhookHandler' in source
    assert 'ReconciliationService' in source


def test_scan_service_is_split_across_component_classes() -> None:
    source = Path('src/backtestforecast/services/scans.py').read_text()
    assert 'ScanJobFactory' in source
    assert 'ScanExecutor' in source
    assert 'ScanPresenter' in source


def test_worker_task_modules_exist() -> None:
    assert Path('apps/worker/app/research_tasks.py').exists()
    assert Path('apps/worker/app/pipeline_tasks.py').exists()
    assert Path('apps/worker/app/worker_maintenance_tasks.py').exists()
