"""Background task system for Sergo.

Provides fire-and-forget tasks and recurring background loops
that run in the same asyncio event loop as the FastAPI server.

Usage:
    from sergo.tasks import TaskLoop

    loop = TaskLoop()

    @loop.recurring(interval=60)
    async def check_reminders():
        due = get_due_reminders()
        for r in due:
            loop.add_task(process_reminder(r))

    @loop.recurring(interval=900)
    async def heartbeat():
        await do_heartbeat()

    # In runserver.py
    handler = get_handler()
    handler.configure(task_loop=loop)

    # Anywhere in your app — fire and forget
    from myapp import loop
    loop.add_task(some_async_function(args))
"""

import asyncio
from typing import Callable, Coroutine, Any

from settings import logger


class TaskLoop:
    """Manages background tasks and recurring loops.

    All tasks run in the same asyncio event loop as the server.
    Started automatically when passed to handler.configure().
    """

    _instance = None

    def __init__(self):
        self._recurring: list[tuple[float, Callable, str]] = []
        self._queue: asyncio.Queue | None = None
        self._tasks: list[asyncio.Task] = []
        self._running = False
        TaskLoop._instance = self

    @classmethod
    def get_instance(cls) -> 'TaskLoop':
        """Get the most recently created TaskLoop instance."""
        if cls._instance is None:
            raise RuntimeError("No TaskLoop instance created yet")
        return cls._instance

    def recurring(self, interval: int, name: str | None = None):
        """Decorator to register a recurring background function.

        Args:
            interval: Seconds between each execution.
            name: Optional name for logging. Defaults to function name.
        """
        def wrapper(fn):
            label = name or fn.__name__
            self._recurring.append((interval, fn, label))
            return fn
        return wrapper

    def add_task(self, coro: Coroutine[Any, Any, Any]) -> None:
        """Fire-and-forget a coroutine. Safe to call from sync or async code.

        The coroutine is queued and executed in the background.
        Errors are logged but don't crash the server.
        """
        if self._queue is not None:
            self._queue.put_nowait(coro)
        else:
            logger.warning("TaskLoop not started — task dropped")

    async def start(self) -> None:
        """Start the task loop. Called automatically by handler.configure()."""
        self._running = True
        self._queue = asyncio.Queue()

        # Start recurring loops
        for interval, fn, label in self._recurring:
            self._tasks.append(asyncio.create_task(
                self._run_recurring(interval, fn, label)
            ))
            logger.info(f"TaskLoop: started recurring '{label}' (every {interval}s)")

        # Start task consumer
        self._tasks.append(asyncio.create_task(self._consume()))
        logger.info(f"TaskLoop: started with {len(self._recurring)} recurring tasks")

    async def stop(self) -> None:
        """Stop all background tasks."""
        self._running = False
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()
        self._queue = None
        logger.info("TaskLoop: stopped")

    async def _run_recurring(self, interval: float, fn: Callable, label: str) -> None:
        """Run a function repeatedly with a fixed interval."""
        while self._running:
            try:
                await asyncio.sleep(interval)
                if self._running:
                    await fn()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"TaskLoop recurring '{label}' error: {e}", exc_info=True)

    async def _consume(self) -> None:
        """Process fire-and-forget tasks from the queue."""
        while self._running:
            try:
                coro = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                asyncio.create_task(self._safe_execute(coro))
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

    @staticmethod
    async def _safe_execute(coro: Coroutine) -> None:
        """Execute a coroutine with error handling."""
        try:
            await coro
        except Exception as e:
            logger.error(f"TaskLoop task error: {e}", exc_info=True)
