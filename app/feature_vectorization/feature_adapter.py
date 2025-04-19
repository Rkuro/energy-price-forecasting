from abc import ABC, abstractmethod
from typing import Any, List, Tuple
from .horizons import Horizon

# -----------------------------
# Abstract FeatureAdapter Base
# -----------------------------
class FeatureAdapter(ABC):
    """
    Base class for a data adapter that transforms raw data into a vectorized format.
    Each subclass handles one input type (e.g., weather, fuel, LMP).
    """
    feature_vector_size: int

    @abstractmethod
    def can_handle(self, msg_type: str) -> bool:
        """Return True if this adapter can handle the given message type."""
        pass

    @abstractmethod
    def vectorize(self, data: Any, past_data: Any) -> Tuple[List[Horizon], List[float]]:
        """ Vectorize the input and list horizons affected"""
        pass

    @abstractmethod
    def archive(self, data: Any) -> Any:
        """Return an archived format for this message type"""
        pass

    @abstractmethod
    def unarchive(self, data: Any) -> Any:
        """Return an unarchived format for this message type"""
        pass