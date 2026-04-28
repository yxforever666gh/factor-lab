from factor_lab.question_generator import _epistemic_priority_gain


def test_epistemic_priority_gain_detects_high_information_question():
    policy = {"principles": {"objective": ["epistemic_gain"]}}
    assert _epistemic_priority_gain(["boundary_confirmed"], policy) is True
    assert _epistemic_priority_gain(["no_significant_information_gain"], policy) is False
