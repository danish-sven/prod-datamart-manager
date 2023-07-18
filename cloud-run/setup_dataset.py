import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def setup_master_dataset(dataset_id, project_id, sql_dir):
    """
    Create or update a dataset and its views in BigQuery.

    :param dataset_id: The ID of the dataset to be created or updated.
    :param project_id: The ID of the GCP project.
    :param sql_dir: The directory containing the SQL files for the dataset's views.
    """

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Check if dataset exists, create it if not
    dataset_ref = client.dataset(dataset_id)
    try:
        dataset = client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
         # Set the location for the dataset
        dataset.location = 'australia-southeast1'
        # Create the dataset
        dataset = client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created")

    # Iterate through the SQL files in the given directory
    for root, dirs, files in os.walk(sql_dir):
        print(f"Checking {root} for SQL files...")  # Add this line for debugging
        for file in files:
            if file.endswith('.sql'):
                print(f"Found SQL file: {file}")  # Add this line for debugging
                sql_file = os.path.join(root, file)
                view_name = os.path.splitext(os.path.basename(sql_file))[0]

                # Read SQL file contents
                with open(sql_file) as f:
                    sql = f.read()

                # Check if view already exists in dataset
                view_ref = dataset_ref.table(view_name)
                try:
                    view = client.get_table(view_ref)
                    print(f"View {view_name} already exists, updating it...")
                    view.view_query = sql
                    client.update_table(view, ['view_query'])
                    print(f"View {view_name} updated")
                except NotFound:
                    # Create view in dataset
                    view = bigquery.Table(view_ref)
                    view.view_query = sql
                    view.view_use_legacy_sql = False
                    view = client.create_table(view)
                    print(f"View {view_name} created")

    print(f"Master dataset {dataset_id} setup successfully")