from __future__ import annotations
import os
import re
import math
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from array import array

FROM_LINE_RE = re.compile(rb"^From ")


class MboxOffsetMap:
    '''
    A collection with magic methods representing the byte offsets of each of the emails 
    in a MBOX file.
    '''

    def __init__(self, mbox_path: str, offsets: Optional[array] = None) -> None:
        '''
        mbox_path: Path to the mbox file.
        offsets: Optional array('Q') of offsets.
        '''
        self.mbox_path: str = mbox_path
        self.offsets: array = offsets if offsets is not None else array('Q')

    def __getitem__(self, index: int) -> int:
        '''Returns the offset at the specified index.'''
        return self.offsets[index]

    def __len__(self) -> int:
        '''Returns the total number of offsets.'''
        return len(self.offsets)

    @classmethod
    def create(cls, mbox_path, threads=1) -> MboxOffsetMap:
        '''Creates an offset map given a path to an MBOX file.'''
        return _OffsetInitializer.create(mbox_path, threads)

    def export(self, file_path: str) -> None:
        '''Exports the offset map to a binary file.'''
        _OffsetInitializer.export(self, file_path)

    @classmethod
    def import_offsets(cls, mbox_path: str, bin_file: str) -> MboxOffsetMap:
        '''Imports an offset map from a binary file.'''
        return _OffsetInitializer.import_(
            mbox_path, 
            bin_file
        )


class _OffsetInitializer:
    '''Utility class for creating and managing MboxOffsetMap instances.'''

    @staticmethod
    def create(mbox_path: str, threads: int = 4) -> MboxOffsetMap:
        '''Creates an offset map by scanning the mbox file.

        Args:
            mbox_path: Path to the mbox file.
            threads: Number of threads for creating the offset map, default is 4.

        Returns:
            A MboxOffsetMap instance with offsets built.
        '''
        file_size = os.path.getsize(mbox_path)
        if file_size == 0:
            return MboxOffsetMap(mbox_path, array('Q'))

        if threads <= 1:
            offsets_list: List[int] = []
            current_offset = 0
            with open(mbox_path, "rb") as f:
                for line in f:
                    if FROM_LINE_RE.match(line):
                        offsets_list.append(current_offset)
                    current_offset += len(line)
        else:
            chunk_size = math.ceil(file_size / threads)
            results: List[int] = []
            with ThreadPoolExecutor(max_workers=threads) as executor:
                futures = []
                for i in range(threads):
                    start = i * chunk_size
                    end = min(file_size, (i + 1) * chunk_size)
                    if start >= file_size:
                        break
                    futures.append(
                        executor.submit(
                            _OffsetInitializer._scan_chunk,
                            mbox_path,
                            start,
                            end
                        )
                    )
                for future in as_completed(futures):
                    results.extend(future.result())
            offsets_list = sorted(set(results))

        offsets_arr = array('Q', offsets_list)
        return MboxOffsetMap(mbox_path, offsets_arr)

    @staticmethod
    def _scan_chunk(mbox_path: str, chunk_start: int, chunk_end: int) -> List[int]:
        '''Scans a chunk of the mbox file for message boundaries.

        Args:
            mbox_path: Path to the mbox file.
            chunk_start: Starting byte offset.
            chunk_end: Ending byte offset.

        Returns:
            A list of absolute offsets for messages found in the chunk.
        '''
        offsets: List[int] = []
        with open(mbox_path, "rb") as f:
            f.seek(chunk_start)
            if chunk_start != 0:
                f.readline()  # Discard partial line.
                chunk_start = f.tell()
            current_offset = chunk_start
            while current_offset < chunk_end:
                line = f.readline()
                if not line:
                    break
                if FROM_LINE_RE.match(line):
                    offsets.append(current_offset)
                current_offset += len(line)
                if current_offset >= chunk_end:
                    break
        return offsets

    @staticmethod
    def export(mbox_offset_map: MboxOffsetMap, file_path: str) -> None:
        '''Exports the offset map to a binary file.

        Args:
            mbox_offset_map: The offset map to export.
            file_path: Destination binary file path.
        '''
        arr = mbox_offset_map.offsets
        count = len(arr)
        with open(file_path, "wb") as f:
            header = array('Q', [count])
            f.write(header.tobytes())
            arr.tofile(f)

    @staticmethod
    def import_(mbox_path: str, bin_file: str) -> MboxOffsetMap:
        '''Imports an offset map from a binary file.

        Args:
            mbox_path: Path to the mbox file.
            bin_file: Path to the binary file with exported offsets.

        Returns:
            A MboxOffsetMap instance with imported offsets.
        '''
        instance = MboxOffsetMap(mbox_path)
        with open(bin_file, "rb") as f:
            header = array('Q')
            header.fromfile(f, 1)
            count = header[0]
            offsets_arr = array('Q')
            offsets_arr.fromfile(f, count)
            instance.offsets = offsets_arr
        return instance
