from __future__ import annotations
from dataclasses import dataclass
import datetime

@dataclass
class Person():
    id: int
    age: int
    dob: datetime.date
    gender: str

    def __str__(self) -> str:
        return f"ID: {self.id}, Age: {self.age}, DoB: {self.dob}, Gender: {self.gender}"