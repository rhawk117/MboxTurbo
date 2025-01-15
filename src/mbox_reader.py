

from __future__ import annotations

from typing import Callable, Generic, List, Optional, TypeVar
from concurrent.futures import (
    Executor,
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    Future,
    as_completed,
)

from .mbox_engine import MboxTurboEngine, T
from .offset import MboxOffsetMap


class MboxReader(MboxTurboEngine[T], Generic[T]):

    def __init__(
        self,
        mbox_path: str,
        offset_map: Optional[MboxOffsetMap] = None,
        factory: Optional[Callable[[bytes], T]] = None,
        chunk_size: int = 1,
        concurrency: str = "thread",  # "none", "thread", or "process"
        num_workers: int = 4,
        process_message: Optional[Callable[[T], None]] = None,
        process_chunk: Optional[Callable[[List[T]], None]] = None,
    ) -> None:
        """
        mbox_path: Path to the .mbox file
        offset_map: Optional existing MboxOffsetMap
        factory: raw_bytes -> T
        chunk_size: How many messages per chunk (default 1)
        concurrency: "none", "thread", or "process"
        num_workers: number of threads/processes for parallel exec
        process_message: callback for individual messages
        process_chunk: callback for chunk of messages
        """
        super().__init__(
            mbox_path,
            offset_map=offset_map,
            factory=factory
        )

        self.chunk_size = max(1, chunk_size)
        self.concurrency = concurrency
        self.num_workers = max(1, num_workers)

        self.process_message = process_message
        self.process_chunk = process_chunk

    def _get_executor(self) -> Optional[Executor]:
        if self.concurrency == "none":
            return None

        elif self.concurrency == "thread":
            return ThreadPoolExecutor(max_workers=self.num_workers)

        elif self.concurrency == "process":
            return ProcessPoolExecutor(max_workers=self.num_workers)

        else:
            raise ValueError(f"Unknown concurrency mode: {self.concurrency}")

    def begin(self, start_idx: int = 0, end_idx: Optional[int] = None, threads_for_index: int = 1) -> None:
        """
        1) build the offset map, if omitted by caller (possibly multi-threaded).
        2) Read messages from [start_idx:end_idx] in chunks of `chunk_size`,
           optionally processing them in parallel (thread or process pool).
        """
        self.init_offsets(threads=threads_for_index)

        total_msgs = self.get_message_total()
        if end_idx is None or end_idx >= total_msgs:
            end_idx = total_msgs - 1

        if end_idx < start_idx:
            return

        chunks = []
        current = start_idx
        while current <= end_idx:
            c_end = min(current + self.chunk_size - 1, end_idx)
            chunks.append((current, c_end))
            current += self.chunk_size

        executor = self._get_executor()
        if executor is None:
            for (c_start, c_end) in chunks:
                self._process_chunk_synchronously(c_start, c_end)
        else:
            # parallel
            futures: List[Future] = []
            for (c_start, c_end) in chunks:
                fut = executor.submit(
                    self._process_chunk_synchronously,
                    c_start,
                    c_end
                )
                futures.append(fut)

            for fut in futures:
                fut.result()

            executor.shutdown()

    def _process_chunk_synchronously(self, chunk_start: int, chunk_end: int) -> None:
        """
        [chunk_start:chunk_end], then calls either
        - process_chunk(...) if chunk_size > 1 and provided, or
        - process_message(...) for each message if chunk_size=1 or fallback.
        """
        messages = list(self.iter_range(chunk_start, chunk_end))
        if self.process_chunk and self.chunk_size > 1:
            self.process_chunk(messages)

        else:
            if self.process_message:
                for msg in messages:
                    self.process_message(msg)
