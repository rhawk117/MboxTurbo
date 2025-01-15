from __future__ import annotations

from typing import Callable, Generic, Iterator, Optional, Sequence, TypeVar
from io import BytesIO
from email import policy
from email import message_from_binary_file

from .offset import MboxOffsetMap

T = TypeVar("T")


def default_factory(raw_bytes: bytes):
    """
    default factory: parse raw bytes into a Python EmailMessage using
    `message_from_binary_file(..., policy=policy.default)`.
    """
    with BytesIO(raw_bytes) as buf:
        return message_from_binary_file(
            buf,
            policy=policy.default  # type: ignore
        )


class MboxTurboEngine(Generic[T]):
    def __init__(
        self,
        mbox_path: str,
        offset_map: Optional[MboxOffsetMap] = None,
        factory: Optional[Callable[[bytes], T]] = None,
    ) -> None:
        """
        mbox_path: Path to the .mbox file.
        offset_map: A pre-built MboxOffsetMap, or None to create one automatically.
        factory: raw_bytes -> T. default creates an EmailMessage object. 
        """
        self.mbox_path: str = mbox_path
        self.factory: Callable[[bytes],
                               T] = factory or default_factory  # type: ignore

        if offset_map is not None:
            self.offset_map = offset_map

        else:
            self.offset_map = MboxOffsetMap(mbox_path)

    def init_offsets(self, threads: int = 1) -> None:
        """builds the offset map if not already built, specify multi-threading."""
        self.offset_map.build(threads=threads)

    def get_message_total(self) -> int:
        return len(self.offset_map)

    def read_message(self, idx: int) -> T:
        """gets the message at index `idx`"""
        if idx < 0 or idx >= len(self.offset_map):
            raise IndexError(f"Message index {idx} out of range.")

        start_offset = self.offset_map[idx]
        end_offset = None
        if idx + 1 < len(self.offset_map):
            end_offset = self.offset_map[idx + 1]

        with open(self.mbox_path, "rb") as f:
            f.seek(start_offset)
            if end_offset is not None:
                raw_bytes = f.read(end_offset - start_offset)
            else:
                raw_bytes = f.read()

        return self.factory(raw_bytes)

    def iter_range(self, start_idx: int, end_idx: int) -> Iterator[T]:
        """
        iterator for the a range of email msgs

        start_idx: the index of the first mbox message to read
        end_idx: the index of the last mbox message to read
        """
        if start_idx < 0:
            start_idx = 0

        if end_idx >= len(self.offset_map):
            end_idx = len(self.offset_map) - 1

        for i in range(start_idx, end_idx + 1):
            yield self.read_message(i)

    def iter_indices(self, indices: Sequence[int]) -> Iterator[T]:
        """yields messages for a specific list/sequence of indices."""
        for i in indices:
            yield self.read_message(i)
