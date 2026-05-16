from factor_lab.llm_pricing import estimate_llm_cost_usd, pricing_for_model


def test_pricing_for_model_matches_gpt_and_opus_families():
    assert pricing_for_model("gpt-5.5")["pricing_family"] == "gpt-5.5"
    assert pricing_for_model("gpt-5")["pricing_family"] == "gpt-5"
    assert pricing_for_model("opus4.7")["pricing_family"] == "opus4.7"
    assert pricing_for_model("claude-opus-4.7-20260428")["pricing_family"] == "opus4.7"


def test_estimate_llm_cost_usd_uses_gpt_cached_input_rate():
    cost = estimate_llm_cost_usd(
        "gpt-5.5",
        {
            "prompt_tokens": 1000,
            "cached_tokens": 800,
            "completion_tokens": 100,
        },
    )

    assert cost["pricing_family"] == "gpt-5.5"
    assert cost["uncached_input_tokens"] == 200
    assert cost["cached_input_tokens"] == 800
    # GPT-5 default: $1.25/M uncached input, $0.125/M cached input, $10/M output.
    assert cost["estimated_cost_usd"] == 0.00135


def test_estimate_llm_cost_usd_uses_opus_cache_creation_and_read_rates():
    cost = estimate_llm_cost_usd(
        "opus4.7",
        {
            "prompt_tokens": 1000,
            "cache_creation_tokens": 500,
            "cached_tokens": 2000,
            "completion_tokens": 100,
        },
    )

    assert cost["pricing_family"] == "opus4.7"
    assert cost["uncached_input_tokens"] == 1000
    assert cost["cache_creation_tokens"] == 500
    assert cost["cached_input_tokens"] == 2000
    # Opus default: $15/M input, $18.75/M 5m cache write, $1.50/M cache read, $75/M output.
    assert cost["estimated_cost_usd"] == 0.034875
