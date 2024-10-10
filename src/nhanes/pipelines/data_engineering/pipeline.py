"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.19.8
"""
from kedro.pipeline import Pipeline, pipeline, node
from .nodes import *

def create_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                func=carga_datasets,
                inputs=None,
                outputs=["demografia", "colesterol", "insulina", "depresion", "proteinaC", "perfilBioquimico", "presionArterial", "medidasCorporales"],
                name="datelis",
            ),
            node(
                func=demografia_completa,
                inputs="demografia",
                outputs="demografia_clean",
                name="MyNode"
            ),
        ]
    )