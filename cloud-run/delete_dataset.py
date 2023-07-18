from google.cloud import bigquery

def delete_removed_datasets(project_id, local_datasets):
    """
    Delete datasets in BigQuery that are not present in the local 'sql' folder.

    :param project_id: The ID of the GCP project.
    :param local_datasets: A list of local dataset folder names.
    """

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Get a list of remote datasets in the project
    remote_datasets = list(client.list_datasets())

    # Iterate through remote datasets and delete those not in the local_datasets list
    for remote_dataset in remote_datasets:
        if remote_dataset.dataset_id not in local_datasets:
            dataset_ref = client.dataset(remote_dataset.dataset_id)
            client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
            print(f"Dataset {remote_dataset.dataset_id} deleted")