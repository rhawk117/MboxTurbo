from __future__ import annotations

import os
import re
import math
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from array import array

MSG_START = re.compile(rb"^From ")


class MboxOffsetMap:
    """
    builds a sorted list of message start offsets by scanning an mbox file.

    feats:
      - multi-threading to accelerate chunk scanning.
      - exporting/importing offsets to/from a *binary* file.
      -  __getitem__() for getting nth message offset and __len__() for
        total messages.
    """

    def __init__(self, mbox_path: Optional[str] = None) -> None:
        self.mbox_path: Optional[str] = mbox_path
        self.offsets: Optional[List[int]] = None

    def build(self, threads: int = 1) -> None:
        """
        searches mbox file and finds every "From " boundary.
        if 'threads' > 1, attempts a simple multi-threaded approach by
        dividing the file into chunks.
        """
        if self.mbox_path is None:
            raise ValueError(
                "[MBOX_TURBO] - Cannot create offset map without an mbox path.")

        if self.offsets is not None:
            return

        file_size = os.path.getsize(self.mbox_path)
        if file_size == 0:
            self.offsets = []
            return

        if threads <= 1:
            self._unthreaded_builder()
        else:
            self._threaded_builder(threads)

        if self.offsets:
            self.offsets = sorted(set(self.offsets))

    def _unthreaded_builder(self) -> None:
        """no multi-threaded, linear traversal of the entire mbox. """
        self.offsets = []
        current_offset = 0
        global MSG_START
        if self.mbox_path is None:
            raise ValueError(
                "[MBOX_TURBO] - Cannot create offset map without an mbox path.")

        with open(self.mbox_path, "rb") as f:
            for line in f:
                if MSG_START.match(line):
                    self.offsets.append(current_offset)
                current_offset += len(line)

    def _threaded_builder(self, threads: int) -> None:
        """
        splits the file into roughly equal chunks of bytes, 
        each scanned in a separate thread.
        """

        file_size = os.path.getsize(self.mbox_path)
        chunk_size = math.ceil(file_size / threads)

        results = []
        with ThreadPoolExecutor(max_workers=threads) as executor:
            exc_results = []
            for i in range(threads):
                start = i * chunk_size
                end = min(file_size, (i + 1) * chunk_size)

                if start >= file_size:
                    break

                exc_task = executor.submit(
                    self._scan_chunk,
                    start,
                    end
                )
                exc_results.append(exc_task)

            for offet_results in as_completed(exc_results):
                chunk_offsets = offet_results.result()
                results.extend(chunk_offsets)

        self.offsets = results

    def _scan_chunk(self, chunk_start: int, chunk_end: int) -> List[int]:
        """
        reads [chunk_start:chunk_end) of the file, scanning line by line
        to find lines that start with "From "; returns the absolute offsets.
        """
        offsets = []
        global MSG_START
        if chunk_start > chunk_end:
            return offsets
        
        with open(self.mbox_path, "rb") as f:
            f.seek(chunk_start)
            offset_idx = chunk_start

            while offset_idx < chunk_end:
                line = f.readline()

                if not line:
                    break

                if MSG_START.match(line):
                    offsets.append(offset_idx)

                offset_idx += len(line)

                if offset_idx >= chunk_end:
                    break
        return offsets

    def __getitem__(self, index: int) -> int:
        """gets the offset of the nth item of an mbox"""
        if self.offsets is None:
            raise RuntimeError("Offsets not built or imported yet.")
        return self.offsets[index]

    def __len__(self) -> int:
        """number of offsets (messages) stored."""
        return 0 if (self.offsets is None) else len(self.offsets)

    def export_bin(self, file_path: str) -> None:
        """
        exports class stored offsets to a binary file,
        the file can have any name or extension 
        notes:
          - keeps an 8-byte 'count' (unsigned 64-bit),
          - this count is followed by that many 8-byte 
            offsets (array of 'Q').
        """
        if self.offsets is None:
            raise RuntimeError(
                "No offsets to export. Call build() first or import them."
            )
        arr = array('Q', self.offsets)
        count = len(arr)

        with open(file_path, mode="wb") as f:
            header = array('Q', [count])
            f.write(header.tobytes())
            arr.tofile(f)

    @classmethod
    def import_bin(cls, bin_file: str) -> MboxOffsetMap:
        """imports offsets from a binary file thats been exported export_bin()"""
        instance = cls()
        with open(bin_file, "rb") as f:
            header = array('Q')
            header.fromfile(f, 1)
            count = header[0]

            offsets_arr = array('Q')
            offsets_arr.fromfile(f, count)

            instance.offsets = list(offsets_arr)

        return instance
