from __future__ import annotations
from pathlib import Path
from typing import Any, Type

from .artifacts import Artifact
from .producers import get_producer
from .deps import Deps
from .config import RunConfig

class Executor:
    """
    Executor must materialise the artifacts applying same configurations defined for the whole workflow.
    It knows RunConfig(with splitting strategy and percentage) and cache directory.
    """    
    def __init__(self, cache_dir: Path, config: RunConfig):
        self.cache_dir = cache_dir
        self.config = config
    

    def path_for(self, art: Artifact) -> Path:
        """
        Fileset:
            .cache/Fileset/<identity>/fileset.json
        
        Chunking:
            .cache/Chunking/<identity>/ 
                manifest.json
                fileset_datasetA.json
                fileset_datasetB.json
            OR
            .cache/Chunking/<identity>/
                manifest.json
                fileset_0_20_percent.json
                fileset_20_40_percent.json
                ...
        
        ChunkAnalysis:
            .cache/ChunkAnalysis/<chunk_identity>/payload.pkl
                                /<chunk_identity>/payload.pkl
                                /<chunk_identity>/payload.pkl
        
        Analysis (merged analysis):
            .cache/Analysis/<analysis_identity>/payload.pkl
        """
        return self.cache_dir / art.type_name / art.identity() 

    def exists(self, art: Artifact) -> bool:
        """
        TODO
        It only check and Artifact folder creation, but not the output files existence!!!
        """
        return self.path_for(art).exists()


    def materialize(self, art: Artifact) -> Path:
        out = self.path_for(art)
        if not getattr(art, "always_rerun", False) and out.exists():
            return out

        fn = get_producer(type(art))
        deps = Deps(self)             
        fn(art=art, deps=deps, out=out, config=self.config)

        if not out.exists():
            raise RuntimeError(
                f"Producer for {art.type_name} finished but did not create output at {out}"
            )
        return out 