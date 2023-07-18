from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import os

def sync_views(dataset_id, project_id, sql_dir):
    """
    Ensure every .sql file has a view and every view has a .sql file.

    :param dataset_id: The ID of the dataset to be synced.
    :param project_id: The ID of the GCP project.
    :param sql_dir: The directory containing the SQL files for the dataset's views.
    """

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Get the dataset
    dataset_ref = client.dataset(dataset_id)
    try:
        dataset = client.get_dataset(dataset_ref)
    except NotFound:
        print(f"Dataset {dataset_id} does not exist")
        return

    # Get a list of view names in the dataset
    views = [table.table_id for table in client.list_tables(dataset)]

    # Iterate through the SQL files in the given directory
    sql_files = []
    for root, dirs, files in os.walk(sql_dir):
        for file in files:
            if file.endswith('.sql'):
                sql_file = os.path.join(root, file)
                view_name = os.path.splitext(os.path.basename(sql_file))[0]
                sql_files.append(view_name)

                # If the .sql file does not have a corresponding view, print a message
                if view_name not in views:
                    print(f".sql file {file} does not have a corresponding view")

    # Check if views have a corresponding .sql file, if not delete them
    for view_name in views:
        if view_name not in sql_files:
            view_ref = dataset_ref.table(view_name)
            client.delete_table(view_ref, not_found_ok=True)
            print(f"View {view_name} does not have a corresponding .sql file and was deleted")
