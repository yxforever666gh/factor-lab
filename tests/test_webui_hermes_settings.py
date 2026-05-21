from fastapi.testclient import TestClient

from factor_lab import webui_app


def test_hermes_page_replaces_agents_page():
    client = TestClient(webui_app.app)
    response = client.get("/hermes")
    assert response.status_code == 200
    assert "Hermes" in response.text


def test_agents_page_redirects_to_hermes():
    client = TestClient(webui_app.app)
    response = client.get("/agents", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/hermes"
