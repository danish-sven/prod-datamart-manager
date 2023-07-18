"""
Authorise Views 

__author__ = "Martin Barter"
__contributor__ = "Stefan Hall"
__date__ = "2020-02-05"
__version__ = "0.3"

    Version:
        0.3 : Simplified main block and imports - SH
        0.2 : Added Argparse to function - MB
        0.1 : Created fwc - MB
        
__summary__: "This function is designed to authorise views in another project/s in GCP. The script does not create views, but only authorises them to be queried by users outside of a project.
"""

import argparse
from google.cloud import bigquery
from bqva import ViewAnalyzer

def auth_views(action, project, datasets=list()):
    """
    Authorizes views in the specified project and datasets.

    Args:
        action (str): "apply" or "revoke" permissions
        project (str): project where the final view is that needs authorization
        datasets (list, optional): list of dataset(s) which need all views inside them authorized.
    """
    # Initialize BigQuery client for the given project
    client = bigquery.Client(project)

    # If no datasets are provided, get the list of all datasets in the project
    if len(datasets) == 0:
        datasets = client.list_datasets()

    # Iterate through the datasets
    for dataset in datasets:
        try:
            dataset = client.get_dataset(dataset)
        except:
            dataset = client.dataset(dataset.dataset_id)

        # Get the list of tables within the dataset
        tables = client.list_tables(dataset.dataset_id)

        # Iterate through the tables
        for table in tables:
            # Skip if the table is not a view
            if table.table_type != "VIEW":
                pass
            else:
                # Initialize ViewAnalyzer for the current view
                view = ViewAnalyzer(
                    project_id=table.project,
                    dataset_id=table.dataset_id,
                    view_id=table.table_id,
                )

                # Apply or revoke permissions based on the action argument
                if action == "apply":
                    view.apply_permissions()
                    print(
                        f"Views authorized for {table.project}.{table.dataset_id}.{table.table_id} in project {project}."
                    )
                elif action == "revoke":
                    view.revoke_permissions()
                    print(
                        f"Views revoked for {table.project}.{table.dataset_id}.{table.table_id} in project {project}."
                    )
                else:
                    print("No action specified, please apply or revoke permissions.")

    print(f"Authorization complete for project {project}.")

if __name__ == "__main__":
    # Define command line arguments
    parser = argparse.ArgumentParser(description="Authorize views in BigQuery.")
    parser.add_argument("--action", help="action for the script to do, either 'apply'/'revoke'", required=True)
    parser.add_argument("--project", help="The project to authorize views in.", required=True)
    parser.add_argument("--datasets", nargs="*", default=[], help="A list of dataset(s) which need all views inside them authorized.")
    args = parser.parse_args()

    # Call the auth_views function with the provided arguments
    auth_views(args.action, args.project, datasets=args.datasets)

    print("Done.")
