"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.19.8
"""

from kedro.pipeline import Pipeline, pipeline


from kedro.pipeline import Pipeline, node
from nhanes.nodes.demographic import download_file

def create_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                func=download_file,
                inputs=dict(
                    url="params:nhanes_url",
                    output_path="params:nhanes_output_path"
                ),
                outputs=None,
                name="download_nhanes_data",
            ),
        ]
    )