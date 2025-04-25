from abc import ABC, abstractmethod
from typing import Any, List, Tuple
import os
import json
from datetime import datetime, timezone
from .horizons import Horizon
from ..config import Config


# -----------------------------
# Abstract FeatureAdapter Base
# -----------------------------
class FeatureAdapter(ABC):
    """
    Base class for a data adapter that transforms raw data into a vectorized format.
    Each subclass handles one input type (e.g., weather, fuel, LMP).
    """
    feature_vector_size: int
    training_data_volume_path: str

    def __init__(self, config:Config, message_type: str):
        self.training_data_volume_path = config.training.training_data_volume_path
        self.message_type = message_type

        # Ensure archive path exists
        os.makedirs(os.path.join(
            self.training_data_volume_path,
            self.message_type
        ), exist_ok=True)

    @abstractmethod
    def can_handle(self, msg_type: str) -> bool:
        """Return True if this adapter can handle the given message type."""
        pass

    @abstractmethod
    def vectorize(self, data: Any, past_data: Any) -> Tuple[List[Horizon], List[float]]:
        """ Vectorize the input and list horizons affected"""
        pass

    @abstractmethod
    def archive(self, data: Any) -> None:
        """Save an archived format for this message type"""
        with open(
            os.path.join(self.training_data_volume_path,
                        self.message_type,
                         f"{datetime.now(tz=timezone.utc).isoformat()}.json"),
            "w"
        ) as output_f:
            json.dump(data, output_f)
