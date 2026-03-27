from dataclasses import dataclass
from pathlib import Path
from typing import Literal

SplitStrategy = Literal["by_dataset"] | None

@dataclass(frozen=True)
class RunConfig:
    """
    Defines how to run the analysis:
        - strategy: "by_dataset" splits into one chunk per dataset; None keeps all datasets together
        - percentage: what percent of each dataset's files per chunk (e.g. 20 → 5 chunks); None = no file split
        - datasets: restrict to specific dataset names; accepts list (auto-converted to tuple) or None for all
        - cache_dir: where to put cached outputs
    """
    strategy: SplitStrategy = None
    percentage: int | None = None
    datasets: tuple[str, ...] | None = None
    cache_dir: Path = Path(".cache")

    def __post_init__(self):
        if self.strategy not in (None, "by_dataset"):
            raise ValueError(
                f"Invalid strategy={self.strategy!r}. Use 'by_dataset' or None."
            )

        if self.percentage is not None:
            if not isinstance(self.percentage, int):
                raise TypeError("percentage must be an int")
            if not (1 <= self.percentage <= 100):
                raise ValueError("percentage must be between 1 and 100")
            if 100 % self.percentage != 0:
                raise ValueError(
                    "percentage must divide 100 evenly (e.g. 10, 20, 25, 50)."
                )

        # auto-convert list → tuple for hashability
        if isinstance(self.datasets, list):
            object.__setattr__(self, "datasets", tuple(self.datasets))