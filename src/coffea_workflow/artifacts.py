from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping, Protocol,runtime_checkable
from .identity import hash_identity


ARTIFACT_REGISTRY = {}

def register_artifact(cls):
    ARTIFACT_REGISTRY[cls.__name__] = cls
    return cls

@runtime_checkable
class Artifact(Protocol):
    def keys(self) -> Mapping[str, Any]: ...
    def identity(self) -> str: ...
    def to_dict(self) -> dict: ...
    @property
    def type_name(self) -> str: ...

@dataclass(frozen=True)
class ArtifactBase:
    def keys(self):
        raise NotImplementedError

    @property
    def type_name(self) -> str:
        return type(self).__name__

    def identity(self) -> str:
        return hash_identity(self.to_dict())

    def to_dict(self) -> dict:
        return {"type": self.__class__.__name__, "keys": self.keys()}

@register_artifact
@dataclass(frozen=True)
class Fileset(ArtifactBase):
    """
    External artifact that user uses and defines the builder(his/her cutom script that returns fileset.json)
    """
    name: str
    builder: str # uses function that user provides

    def keys(self) -> Mapping[str, Any]:
        return {
            "name": self.name,
            "builder": self.builder,
        }

@register_artifact
@dataclass(frozen=True)
class Chunking(ArtifactBase):
    """
    Internal artifact. User doesn't use this artifact, it's a helping artifact that is
    produced by Analysis artifact(external) to execute splitting strategy.
    Returns fileset chunks based on splitting strategy.
    Its producer writes:
        .cache/Chunking/<identity>/
            manifest.json
            fileset_chunk_0.json
            fileset_chunk_1.json
            ...
    """
    fileset: Fileset
    split_strategy: str | None
    percentage: int | None
    datasets: tuple[str, ...] | None = None

    def keys(self):
        return {
            "fileset": self.fileset,
            "split_strategy": self.split_strategy,
            "percentage": self.percentage,
            "datasets": self.datasets,
        }

@register_artifact
@dataclass(frozen=True)
class ChunkAnalysis(ArtifactBase):
    """
    Internal artifact. User doesn't use this artifact, it's a helping artifact that is produced 
    by Analysis artifact to process one chunk.
    Its producer's output example:
        - coffea acc (this should be the outcome of user specified builder)
        - .cache/ChunkAnalysis/<chunk_identity>/payload.pkl
    This file is immediately merged to the previous chunk coffea accumulator in Analysis.
    If analysis breaks then it doesn't create an output for that chunk. But Analysis artifact continues the processing of the rest.
    And will identify the missing one when rerunning analysis again.
    """
    chunk_file: str
    chunking: Chunking
    analysis_builder: str


    def keys(self):
        return {
            "chunk_file": self.chunk_file,
            "chunking": self.chunking,
            "analysis_builder": self.analysis_builder,
        }

# most probably is not needed
# @register_artifact
# class Merging(ArtifactBase):
#     """
#     User doesn't use this artifact, it's a helping artifact that is produced by Analysis artifact to merge succesfully processed chunks.
#     ?????????????????
#     """
#     analysis_chunks: str[Path] # path to the folder with chunks to merge
#     parameter: str # how to merge? what to merge?

#     def keys(self):
#         return {
#             "analysis_chunks": self.analysis_chunks,
#             "parameter": self.parameter,
#         }

@register_artifact
@dataclass(frozen=True)
class Analysis(ArtifactBase):
    """
    External artifact. Its producer triggers internal Artifact materialization
    of Chunking artifact first. Iterates through gotten chunks and perfoms ChunkAnalysis.
    Ignores failers per chunk but tracks failed chunks.
    Merges whatever was successfully processed in ChunkAnalysis into one output. 
    Returns merged output and report failed chunks.
    - .cache/Analysis/<analysis_identity>/payload.pkl (with merged output)
    """
    name: str
    fileset: Fileset
    builder: str

    def keys(self):
        return {
            "name": self.name,
            "fileset": self.fileset,
            "builder": self.builder
        }

@register_artifact
@dataclass(frozen=True)
class Plotting(ArtifactBase):
    pass