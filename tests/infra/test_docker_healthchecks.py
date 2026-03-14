"""Test that Docker healthchecks are configured."""
import pytest


def test_worker_healthcheck_in_compose():
    with open("docker-compose.prod.yml") as f:
        content = f.read()
    assert "healthcheck" in content, "docker-compose.prod.yml must have healthchecks"
    worker_section = content[content.index("worker:"):]
    worker_end = worker_section.find("\n  web:") if "\n  web:" in worker_section else len(worker_section)
    worker_section = worker_section[:worker_end]
    assert "healthcheck" in worker_section, "Worker service must have a healthcheck"


def test_web_healthcheck_in_compose():
    with open("docker-compose.prod.yml") as f:
        content = f.read()
    web_section = content[content.index("web:"):]
    web_end = web_section.find("\n  redis:") if "\n  redis:" in web_section else len(web_section)
    web_section = web_section[:web_end]
    assert "healthcheck" in web_section, "Web service must have a healthcheck"
