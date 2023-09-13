from swpt_pythonlib.multiproc_utils import (
    spawn_worker_processes, ThreadPoolProcessor, HANDLED_SIGNALS,
    try_unblock_signals,
)


def test_spawn_worker_processes():

    def _quit():
        assert len(HANDLED_SIGNALS) > 0
        try_unblock_signals()

    spawn_worker_processes(
        processes=2,
        target=_quit,
    )


def test_thread_pool_processor(app):
    n = 0

    def get_args_collection():
        return [(1,), (2,), (3,), (4,)]

    def process(x):
        nonlocal n
        n += x

    with app.app_context():
        ThreadPoolProcessor(
            2,
            get_args_collection=get_args_collection,
            process_func=process,
            wait_seconds=60.0,
            max_count=2,
        ).run(quit_early=True)

    assert n == 10
