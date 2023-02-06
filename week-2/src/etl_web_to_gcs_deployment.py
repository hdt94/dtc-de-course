import os

from prefect.deployments import Deployment

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
