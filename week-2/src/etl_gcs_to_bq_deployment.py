import os

from prefect.deployments import Deployment

from etl_gcs_to_bq_flow import etl_gcs_to_bq_multiple_months


deployment = Deployment.build_from_flow(
    flow=etl_gcs_to_bq_multiple_months,
    name="parameterized",
    parameters={
        "color": "green",
        "year": 2020,
        "months": [1],
        "dest_table": "",
    },
)
deployment.apply()
