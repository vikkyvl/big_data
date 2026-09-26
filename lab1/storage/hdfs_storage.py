import os
from datetime import datetime


class HDFSStorage:
    def __init__(self):
        user = os.environ["USER"]
        host = "localhost"
        port = 9000
        project_directory = f"/user/{user}/study/study_master_degree/big_data_labs/lab1"
        self.base_path = f"hdfs://{host}:{port}{project_directory}"

    def save(self, data, directory_name):
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"{self.base_path}/{directory_name}/run_{run_id}"

        data.coalesce(1).saveAsTextFile(output_path)

        return output_path