from factor_lab.research_queue import _enqueue_followups_for_workflow


class ExplodingStore:
    def __getattr__(self, name):
        raise AssertionError(f"store should not be touched for bucket-aware followup suppression: {name}")


def test_bucket_aware_workflow_does_not_enqueue_generic_generated_batch_followups():
    task = {"task_id": "t1", "task_type": "workflow", "payload": {"source": "bucket_aware_controlled_validation", "route_id": "value_quality_no_distress", "config_path": "x"}}

    assert _enqueue_followups_for_workflow(ExplodingStore(), task, task["payload"]) == []
