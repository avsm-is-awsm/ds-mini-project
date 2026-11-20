from App.Run import run_app_on_process
from Util.Util import fetch_all_files, strip_name_form_path, append_test_sample, write_test_sample, project_root
from Util.Requests import store_file, retrieve_file

from pathlib import Path

from subprocess import TimeoutExpired

from time import perf_counter

def test_store(filename: str, data: bytes) -> float:

    start_time = perf_counter()

    response = store_file(filename, data)

    stoptime = perf_counter() - start_time 

    if response.status_code != 201:
        raise RuntimeError(f"Store failed: {response.text}")

    return stoptime

def test_retrieve(filename: str, expected_data: bytes) -> float:

    start_time = perf_counter()

    response = retrieve_file(filename)

    stoptime = perf_counter() - start_time 

    if response.status_code != 200:
        raise RuntimeError(f"Store failed: {response.text}")

    if response.content != expected_data:
        raise RuntimeError(f"Retrieved data does not match original file: {filename}")

    return stoptime

if __name__ == "__main__":

    meassure_storage_nodes = [6, 12, 24]
    
    for number_of_storage_nodes in meassure_storage_nodes:

        print(f"\n=== N = {number_of_storage_nodes}: starting system ===")

        system = run_app_on_process(
            RESET_DATABASE = True,
            number_of_storage_nodes = number_of_storage_nodes 
        )

        try:

            result_list = []
            test_files = fetch_all_files()

            for sample_number, file in enumerate(test_files):

                filename = strip_name_form_path(file)

                with open(file, "rb") as fp:
                    data = fp.read()

                upload_speed = test_store(filename, data)
                download_speed = test_retrieve(filename, data)

                result_list = append_test_sample(
                    result_list = result_list,
                    current_sample = sample_number,
                    max_sample = len(test_files),
                    filename = filename,
                    data = data,
                    upload_speed = upload_speed,
                    download_speed = download_speed
                )

                write_test_sample(
                    result_list = result_list, 
                    result_file = f"task1_samples_N{number_of_storage_nodes}.csv"
                )

        finally:
            
            print(f"\n=== N = {number_of_storage_nodes}: stopping system ===")

            system.terminate()

            try:
                system.wait(timeout = 5)

            except TimeoutExpired:

                print("Process did not exit gracefully. Killing it...")
                system.kill()
                system.wait()

