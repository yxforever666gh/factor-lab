from factor_lab.autonomous_strategy_dispatcher import dispatch_once


def safe_state(action):
    return {
        "current_state": "x",
        "recommended_next_step": action,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
    }


def test_dispatcher_blocks_unregistered_action():
    report = dispatch_once(run_id="r", controller_state=safe_state("unknown_action"))
    assert report["dispatch_status"] == "blocked_no_registered_safe_action"
    assert report["command"] is None
    assert report["queue_write_allowed"] is False


def test_dispatcher_blocks_unsafe_controller_state():
    state = safe_state("write_proxy_route_verdict")
    state["queue_write_allowed"] = True
    report = dispatch_once(run_id="r", controller_state=state)
    assert report["dispatch_status"] == "blocked_unsafe_controller_state"
    assert report["command"] is None


def test_dispatcher_blocks_forbidden_action_name():
    report = dispatch_once(run_id="r", controller_state=safe_state("controlled_backtest"))
    assert report["dispatch_status"] == "blocked_no_registered_safe_action"
