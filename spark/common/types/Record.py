from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, TypedDict

from spark.common.utils import get_current_timestamp


class Prediction(TypedDict):
    value: str
    type: Literal["text", "image"]


@dataclass
class Record:
    version: str = field(default_factory=lambda: "1")
    inputs: Dict[str, Prediction] = field(default_factory=dict)
    outputs: Dict[str, Prediction] = field(default_factory=dict)
    feedback_keys: Optional[List[str]] = field(default_factory=list)
    feedback_id: Optional[str] = field(default_factory=lambda: None)
    feedbacks: Optional[Dict[str, str]] = field(default_factory=dict)
    ignore_inputs: Optional[List[str]] = field(default_factory=list)
    timestamp: Optional[int] = field(default_factory=get_current_timestamp)