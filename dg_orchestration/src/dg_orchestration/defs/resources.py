from pathlib import Path
from dagster_dbt import DbtCliResource, DbtProject
import dagster as dg

dbt_proj_directory = Path(__file__).resolve().parent.parent.parent.parent / "dbt_transformations"
print(dbt_proj_directory)
dbt_project = DbtProject(dbt_proj_directory)
dbt_resource = DbtCliResource(project_dir=dbt_project)


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "dbt": dbt_resource,
        },
        #schedules = [daily_load]
    )
