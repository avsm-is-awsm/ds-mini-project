from pathlib import Path
from os import listdir

import time
import subprocess
import csv

from Util.Requests import hello_world
from requests import RequestException
from statistics import mean, median

project_root = Path(__file__).resolve().parents[1]


def parse_name(filename: str) -> tuple[str, int]:

    """
    Files all have format name_xx.txt - split the name and version

    :param str filename: The name of the file

    :return tuple: First the name of the file, then the version
    """

    name, version_txt = filename.split("_v")     # ['name' 'vX.txt']
    version = version_txt.split(".")[0]

    return name, int(version)


def fetch_all_files() -> list[Path]:
    """
    Return a list with the path to each file

    :return data files: A sorted list with the path to every datafile 
    """
    
    data_folder = project_root / "Dataset"

    assert data_folder.exists, \
        f"Data folder does not exist, please extract to: {project_root}"

    data_files = []

    for file in listdir(data_folder):
        if file.endswith(".txt"):
            data_files.append(
                data_folder / file
            )

    data_files.sort()

    return data_files


def strip_name_form_path(path: Path) -> str:
    """
    Strip the path from the file

    :return file name: The name of the file
    """

    return str(path).split("/")[-1]


def append_test_sample(
        result_list: list[dict], 
        current_sample: int, 
        max_sample: int, 
        filename: str, 
        data: bytes, 
        upload_speed: float, 
        download_speed: float
        ) -> list[dict]:
    """
    Modify the result list - add a new sample and print it to terminal

    :param list[dict] result_list: The list of results to be updated
    :param int current_sample: The sample number
    :param int max_sample: The total number of samples
    :param str filename: The name of the tested file
    :param bytes data: The raw data from the tested file
    :param float upload_speed: The upload speed in milliseconds
    :param float download_speed: The download speed in milliseconds

    :return modified list: The _result_list_ with the new sample added   
    """

    result_list.append({
        "sample": current_sample + 1,
        "file": filename, 
        "size_bytes": len(data),
        "nodes": "N/A",
        "upload_speed": upload_speed, 
        "download_speed": download_speed
    })
    
    print(f"  [{current_sample + 1}/{max_sample}] {filename}: up {upload_speed:.3f}s  down {download_speed:.3f}s")

    return result_list


def write_test_sample(result_list: list[dict], result_file: str):
    """
    Write the final result_list to a csv file

    :param list[dict] result_list: The final list of results
    :param str result_file: The name of csv file to be created
    """

    result_folder = project_root / "Results"
    result_folder.mkdir(exist_ok=True)

    with open(result_folder / result_file, "w", newline = "") as result_csv:
        writer = csv.DictWriter(
            f = result_csv, 
            fieldnames = list(result_list[0].keys())  # 
        )
        writer.writeheader()
        writer.writerows(result_list)


def write_sample_summarize(values):
    #""
    #"avg": mean(values),
    #"median": median(values),
    #"min": min(values),
    #"max": max(values),
    pass


def wait_for_app_process(process: subprocess.Popen, timeout: int = 20):
    """
    Stalls the program until the flask application is running or hits the timeout. \\
    An error is stop the program if the process fails to launch before the timeout

    :param Popen process: The process running the storage system
    :param int timeout: the maximum time to wait in seconds
    """


    start = time.time()

    while time.time() - start < timeout:

        if process.poll() is not None:
            raise RuntimeError("Application process terminated unexpectedly")

        try:
            response = hello_world()

            if response.json() == {'message': 'Hello World!'}:
                return

        except RequestException:
            pass

        time.sleep(0.2)

    raise TimeoutError("Application did not start in time")


if __name__ == "__main__":
    
    files = fetch_all_files()

    print(f"Data files: {len(files)}/50")
    for file in files:
        print(strip_name_form_path(file))