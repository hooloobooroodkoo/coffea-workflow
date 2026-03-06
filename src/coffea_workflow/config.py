from dataclasses import dataclass
from pathlib import Path
from typing import Literal
    
SplitStrategy = Literal["by_dataset", "percentage_per_file"] | None

_ALLOWED_SPLIT_STRATEGIES = {"by_dataset", "percentage_per_file", None}

@dataclass(frozen=True)
class RunConfig:
    """
    Defines how to run the analysis:
        - split_strategy: how the fileset should be split
        - cache_dir: where to put cached outputs
        - percentage: what percent of each dataset to put in the chunk filesets when using "percentage_per_file".
    """
    split_strategy: SplitStrategy = None
    cache_dir: Path = Path(".cache")
    percentage: int = 20

    def __post_init__(self):
        # we only allow these two strategies
        if self.split_strategy not in _ALLOWED_SPLIT_STRATEGIES:
            raise ValueError(
                f"Invalid split_strategy={self.split_strategy}\n"
                f"Allowed values: {_ALLOWED_SPLIT_STRATEGIES}."
            )

        # check the percentage if "percentage_per_file" strategy
        if self.split_strategy == "percentage_per_file":
            if not isinstance(self.percentage, int):
                raise TypeError("percentage must be an int")
            if not (1 <= self.percentage <= 100):
                raise ValueError("percentage must be between 1 and 100")
            if 100 % self.percentage != 0:
                raise ValueError(
                    "percentage must divide 100 evenly (20, 25, 10, etc) "
                    "to define the number of chunks."
                )