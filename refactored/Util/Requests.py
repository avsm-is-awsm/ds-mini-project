import requests

from App.Config import SERVER_IP, SERVER_PORT

import base64


session = requests.Session()


def create_endpoint(path: str = "") -> str:
    """
    creates endpoints for the Flask application

    :param str path: the path to the endoint from the root 'http://SERVER_IP:SERVERPOT/'
    """
    return f"http://{SERVER_IP}:{SERVER_PORT}/{path}"


def print_response(response: requests.Response):
    """
    Simple print of a request response

    :param requests.Response response: The response object
    """

    message = response.json()
    status = response.status_code 

    print(f"[{status}] : {message}")


def hello_world() -> requests.Response:
    """
    Verify the root end point of the Flask application
    """

    endpoint = create_endpoint()

    return session.get(
        endpoint
    )


def store_file(filename: str, data: bytes, mode: str = "full") -> requests.Response: 
    """
    Upload a file to the application

    :param str filename: The name of the file e.g. 'apple_v2.txt'
    :param bytes data: The contents of the file
    :param str mode: The storage mode used 

    :return response: The request response object
    """

    endpoint = create_endpoint("files")
    
    payload = {
        "filename": filename,
        "content_type": "",
        "contents_b64": base64.b64encode(data).decode('utf-8'),
        "mode": mode
    }

    return session.post(
        url = endpoint,
        json = payload
    )


def retrieve_file(filename: str) -> requests.Response:
    """
    Download a file from the application

    :param str filename: The name of the file e.g. 'apple_v2.txt'

    :return response: The request response object
    """

    endpoint = create_endpoint(f"files/{filename}")
    
    return session.get(
        url = endpoint
    )


if __name__ == "__main__":

    # ensure app running in another terminal
    print_response(hello_world())