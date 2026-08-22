from fastapi.testclient import TestClient

from factor_lab import webui_app


def test_removed_hermes_webui_routes_return_404_for_get_and_post():
    client = TestClient(webui_app.app)

    for path in ["/hermes", "/agents"]:
        assert client.get(path).status_code == 404
        assert client.post(path, data={}).status_code == 404


def test_llm_page_has_no_hermes_or_agent_role_controls():
    client = TestClient(webui_app.app)

    response = client.get("/llm")

    assert response.status_code == 200
    assert "Agent Roles" not in response.text
    assert 'href="/agents"' not in response.text
    assert 'href="/hermes"' not in response.text
