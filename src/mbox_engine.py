from __future__ import annotations

from typing import List, Optional, Union, Iterator, Callable, Sequence, TypeVar, Generic
import mmap
import concurrent.futures
from io import BytesIO
from email import policy
from email.message import EmailMessage
from email import message_from_binary_file

import tqdm
from .offset import MboxOffsetMap

T = TypeVar("T")


def default_factory(raw_bytes: bytes) -> EmailMessage:
    '''
        default factory function to convert raw bytes into an EmailMessage object
    '''
    with BytesIO(raw_bytes) as buf:
        return message_from_binary_file(buf, policy=policy.default) # type: ignore


class MboxTurboEngine(Generic[T]):
    '''Reads messages from an mbox file using an offset map.'''

    def __init__(
        self,
        mbox_path: str,
        offset_map: Optional[MboxOffsetMap] = None,
        factory: Optional[Callable[[bytes], T]] = None,
        use_mmap: bool = False,
        cache_size: int = 16,
    ) -> None:
        '''Initializes the MboxTurboEngine.

        Args:
            mbox_path: Path to the mbox file.
            offset_map: Pre-built offset map; if None, one will be created.
            factory: Function to convert raw bytes into a message object.
            use_mmap: If True, use memory mapping for file access.
            cache_size: Maximum number of chunk ranges to cache.
        '''
        self.mbox_path = mbox_path
        self.factory = factory or default_factory
        self.offset_map = offset_map if offset_map is not None else MboxOffsetMap(mbox_path)
        self._use_mmap = use_mmap
        self._cache_size = cache_size
        self._chunk_cache: dict[tuple[int, int], Union[bytes, memoryview]] = {}

        if self._use_mmap:
            self._file = open(mbox_path, "rb")
            self._mmap = mmap.mmap(
                self._file.fileno(),
                0, 
                access=mmap.ACCESS_READ
            )
        else:
            self._file = open(mbox_path, "rb")
            self._mmap = None

    def __enter__(self) -> MboxTurboEngine[T]:
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def open(self) -> MboxTurboEngine[T]:
        '''opens the engine and checks that the offset map has been built.

        Returns:
            The opened MboxTurboEngine instance.

        Raises:
            ValueError: If no messages are found in the mbox.
        '''
        if len(self.offset_map) == 0:
            self.init_offsets()
            if len(self.offset_map) == 0:
                raise ValueError('No messages found in the mbox file.')
        return self

    def close(self) -> None:
        '''closes the underlying I/O context and memory mapping resources.'''
        if self._mmap:
            self._mmap.close()
            self._mmap = None
        if self._file:
            self._file.close()
            self._file = None

    def init_offsets(self, threads: int = 1) -> None:
        '''builds the offset map for the mbox file, highly 
        reccomend to export the offset map to avoid the computational
        overhead of building the offset map every time.

        Args:
            threads: Number of threads to use for indexing.
        '''
        new_map = MboxOffsetMap.create(self.mbox_path, threads)
        self.offset_map = new_map

    def get_message_total(self) -> int:
        '''Returns the total number of messages.'''
        return len(self.offset_map)

    def read_message(self, idx: int) -> T:
        '''Reads and returns the message at the specified index.

        Args:
            idx: Message index.

        Returns:
            The parsed message.

        Raises:
            IndexError: If idx is out of range.
        '''
        if not self._file:
            raise ValueError('MboxTurboEngine: File is not open.')
        if idx < 0 or idx >= len(self.offset_map):
            raise IndexError(f'MboxTurboEngine: Message index {idx} out of range.')
        start = self.offset_map[idx]
        end = self.offset_map[idx + 1] if idx + \
            1 < len(self.offset_map) else None
        if self._mmap is not None:
            raw = self._mmap[start:end] if end else self._mmap[start:]
            return self.factory(bytes(raw)) # type: ignore
        else:
            self._file.seek(start)
            raw = self._file.read(
                end - start
            ) if end else self._file.read()
            return self.factory(raw) # type: ignore

    def _read_chunk_for_range(self, start_idx: int, end_idx: int) -> Union[bytes, memoryview]:
        '''Reads a chunk covering messages from start_idx to end_idx and caches it.

        Args:
            start_idx: Starting message index.
            end_idx: Ending message index.

        Returns:
            The raw bytes (or memoryview) for the chunk.
        '''
        if not self._file:
            raise ValueError('MboxTurboEngine: File is not open.')
        
        key = (start_idx, end_idx)
        if key in self._chunk_cache:
            return self._chunk_cache[key]
        file_start = self.offset_map[start_idx]
        file_end = (self.offset_map[end_idx + 1] 
            if end_idx + 1 < len(self.offset_map) else None
        )
        if self._mmap is not None:
            chunk = (self._mmap[file_start:file_end] 
                if file_end else self._mmap[file_start:]
            )
            
        else:
            self._file.seek(file_start)
            chunk = self._file.read(
                file_end - file_start
            ) if file_end  else self._file.read()
        
        self._chunk_cache[key] = chunk
        
        if len(self._chunk_cache) > self._cache_size:
            self._chunk_cache.pop(
                next(iter(self._chunk_cache))
            )
        return chunk

    def iter_range(self, start_idx: int, end_idx: int) -> Iterator[T]:
        '''yields messages in the specified index range.

        Args:
            start_idx: Starting message index.
            end_idx: Ending message index.

        Yields:
            Parsed messages.
        '''
        if start_idx < 0:
            start_idx = 0
        if end_idx >= len(self.offset_map):
            end_idx = len(self.offset_map) - 1
        if start_idx > end_idx:
            return
        chunk = self._read_chunk_for_range(start_idx, end_idx)
        base = self.offset_map[start_idx]
        for i in range(start_idx, end_idx + 1):
            s = self.offset_map[i] - base
            e = (self.offset_map[i + 1] - base) if i + \
                1 <= end_idx else len(chunk)
            data = chunk[s:e]
            if isinstance(data, memoryview):
                data = data.tobytes()
            yield self.factory(data)   # type: ignore

    def chunk_iterator(self, chunk_size: int) -> Iterator[List[T]]:
        total = self.total_chunks(chunk_size)
        for i in range(total):
            yield list(self.iter_range(
                i * chunk_size, 
                (i + 1) * chunk_size - 1
            ))
    
    def read_messages_by_indices(self, indices: Sequence[int]) -> Iterator[T]:
        '''Yields messages corresponding to the specified indices.

        Args:
            indices: A sequence of message indices.

        Yields:
            Parsed messages.
        '''
        for i in indices:
            yield self.read_message(i)

    def iter_all(self, chunk_size: int = 1000) -> Iterator[T]:
        '''_summary_
        yields all messages in the mbox in chunks.
        Keyword Arguments:
            chunk_size {int} --  (default: {1000})

        Yields:
            Iterator[T] 
        '''
        total = len(self.offset_map)
        i = 0
        while i < total:
            j = min(i + chunk_size - 1, total - 1)
            yield from self.iter_range(i, j)
            i = j + 1

    def iter_all_prefetch(self, chunk_size: int = 1000) -> Iterator[T]:
        '''
        yields all emails and prefetches the next chunk in a background thread.
        Keyword Arguments:
            chunk_size {int} -- the number of emails to read in each chunk (default: {1000})

        Yields:
            Iterator[T]
        '''
        total = len(self.offset_map)
        i = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = None
            while i < total:
                j = min(i + chunk_size - 1, total - 1)
                if not future:
                    future = executor.submit(self._read_chunk_for_range, i, j)
                chunk = future.result()
                next_i = j + 1
                if next_i < total:
                    next_j = min(next_i + chunk_size - 1, total - 1)
                    future = executor.submit(
                        self._read_chunk_for_range, next_i, next_j)
                else:
                    future = None
                base = self.offset_map[i]
                for k in range(i, j + 1):
                    s = self.offset_map[k] - base
                    e = (self.offset_map[k + 1] - base) if k + 1 <= j else len(chunk)
                    data = chunk[s:e]
                    if isinstance(data, memoryview):
                        data = data.tobytes()
                    yield self.factory(data)  # type: ignore
                i = j + 1

    def total_chunks(self, chunk_size: int) -> int:
        '''_summary_
        
        returns the total number of chunks for the given chunk size
        useful for iterating over the chunks sequentially
        Arguments:
            chunk_size {int} -- the size of each chunk

        Raises:
            ValueError: if chunk_size is less than or equal to 0

        Returns:
            int -- the total number of chunks
        '''
        if chunk_size <= 0:
            raise ValueError('MboxTurboEngine: chunk_size must be > 0')
        total = len(self.offset_map)
        return (total + chunk_size - 1) // chunk_size

    def chunkate(self, chunk_size: int = 1000, num_threads: int = 4) -> list[list[T]]:
        '''Converts all emails into a 2D array (list of lists) based on the chunk size,
        using multi-threading to process each chunk concurrently.

        Args:
            chunk_size: Number of messages per chunk.
            num_threads: Number of threads to use for processing chunks.

        Returns:
            A list of lists, where each inner list contains the parsed messages of a chunk.
        '''
        total_msgs = len(self.offset_map)
        total_chunks = self.total_chunks(chunk_size)

        def read_chunk(chunk_index: int) -> list[T]:
            start_index = chunk_index * chunk_size
            end_index = min(total_msgs - 1, (chunk_index + 1) * chunk_size - 1)
            return list(self.iter_range(start_index, end_index))

        chunks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(read_chunk, i) for i in range(total_chunks)]
            
            for fut in tqdm.tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Chunkating emails...",
                unit="chunk"
            ):
                chunks.append(fut.result())
                
        return chunks