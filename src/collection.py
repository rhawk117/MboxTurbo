from .mbox_engine import MboxTurboEngine, T
from typing import Iterator, Callable, Optional
from .offset import MboxOffsetMap


class MboxCollection(MboxTurboEngine[T]):
    def __init__(
        self,
        mbox_path: str,
        offset_map: Optional[MboxOffsetMap] = None,
        factory: Optional[Callable[[bytes], T]] = None,
    ) -> None:
        super().__init__(mbox_path, offset_map=offset_map, factory=factory)
        self._current_index = 0

    def __getitem__(self, index: int) -> T:
        if isinstance(index, int):
            return self.read_message(index)

        elif isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            if step == 1:
                return list(self.iter_range(start, stop - 1))
            else:
                messages = []
                for idx in range(start, stop, step):
                    messages.append(self.read_message(idx))
                return messages

        else:
            raise TypeError(f"Invalid argument type: {type(index)}")

    def __len__(self) -> int:
        return self.get_message_total()

    def __iter__(self) -> Iterator[T]:
        return self.iter_range(0, len(self) - 1)

    def __str__(self) -> str:
        return f"<MboxCollection total={len(self)} mailbox='{self.mbox_path}'>"

    def __repr__(self) -> str:
        return str(self)

    def next(self) -> T:
        if self._current_index >= len(self):
            self._current_index = 0
            raise StopIteration("No more messages in the mailbox.")

        msg = self.read_message(self._current_index)
        self._current_index += 1
        return msg
