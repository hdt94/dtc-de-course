import os

from prefect.deployments import Deployment
from prefect.filesystems import GitHub

from etl_web_to_gcs_flow import etl_web_to_gcs


deployment = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="parameterized",
    parameters={
        "color": "green",
        "year": 2020,
        "month": 1
    },
)
deployment.apply()

# Deployment configuration decoupled from flow import using GitHub block
deployment = Deployment(
    entrypoint="week-2/src/etl_web_to_gcs_flow.py:etl_web_to_gcs",
    flow_name="etl_web_to_gcs",
    name="green-2020-11-github-storage",
    parameters={
        "color": "green",
        "year": 2020,
        "month": 11
    },
    storage=GitHub.load("github-storage"),
    tags=["github-storage"],
)
deployment.apply()

deployment = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="green-2019-04-slack-notify",
    parameters={
        "color": "green",
        "year": 2019,
        "month": 4
    },
    tags=["slack-notify"],
)
deployment.apply()
