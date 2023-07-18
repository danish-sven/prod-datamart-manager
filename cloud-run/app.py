import os
from flask import Flask
from setup_dataset import setup_master_dataset
from delete_dataset import delete_removed_datasets
from views_check import sync_views

app = Flask(__name__)

project_id = 'central-ops-datamart-4fe3'
sql_base_dir = 'cloud-run/sql'

def main_process(): 
    local_datasets = []

    # Iterate through directories in the 'sql' folder
    for dataset_folder in os.listdir(sql_base_dir):
        dataset_path = os.path.join(sql_base_dir, dataset_folder)
        if os.path.isdir(dataset_path):
            local_datasets.append(dataset_folder)
            setup_master_dataset(dataset_folder, project_id, dataset_path)
            sync_views(dataset_folder, project_id, dataset_path)

    # Delete remote datasets not present in the 'sql' folder
    delete_removed_datasets(project_id, local_datasets)
    return 'Datamart Updated'

@app.route('/main', methods=['POST'])
def main():
    return main_process()

if __name__ == "__main__":
    main_process()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
