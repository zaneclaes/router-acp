from scripts.summary import summarize


def test_success_is_preserved():
    assert summarize([{"service": "backend", "status": "success"}]) == [
        {"service": "backend", "status": "success"}
    ]
