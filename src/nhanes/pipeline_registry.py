"""Project pipelines."""

from nhanes.pipelines import data_engineering as de



def register_pipelines():
    data_engineering_pipeline = de.create_pipeline()
    return {
        "__default__": data_engineering_pipeline,
        "de": data_engineering_pipeline,
    }