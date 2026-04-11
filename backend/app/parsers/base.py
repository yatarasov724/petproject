from abc import ABC, abstractmethod
from typing import List
from dataclasses import dataclass
from datetime import datetime


@dataclass
class RawNews:
    source: str
    title: str
    content: str
    url: str
    published_at: datetime


class BaseParser(ABC):
    @abstractmethod
    async def fetch(self) -> List[RawNews]:
        pass
