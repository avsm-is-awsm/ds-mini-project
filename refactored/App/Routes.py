from flask import Blueprint, request, make_response, send_file, current_app

from base64 import b64decode
from io import BytesIO

from Classes.Nodes.LeadNode import LeadNode


main = Blueprint("main", __name__)


def get_lead_node() -> LeadNode:
    return current_app.config["LEAD_NODE"]


@main.route('/')
def hello_world():
    return make_response({'message': 'Hello World!'})


@main.route('/files', methods=['POST'])
def store_file():
    """
    Store a file 
    """

    lead_node = get_lead_node()

    payload = request.get_json()

    filename = payload.get('filename')
    content_type = payload.get('content_type')
    file_data = b64decode(payload.get('contents_b64'))
    mode = payload.get('mode', 'full')

    result = lead_node.store_file(
        filename = filename,
        data = file_data,
        content_type = content_type,
        mode = mode
    )

    result_text = ""
    match result:
        case 200:
            result_text = f"{filename} already exists"
        case 201:
            result_text = f"{filename} was created"
        case 404:
            result_text = f"{filename} could not be stored"

    return make_response(result_text, result)


@main.route('/files/<string:filename>',  methods=['GET'])
def retrieve_file(filename: str):

    lead_node = get_lead_node()

    file_data, database_entry = lead_node.retrieve_file(filename)

    if (not file_data) or (not database_entry):
        return make_response(f"{filename} could not be found", 404)

    return send_file(BytesIO(file_data), mimetype=str(database_entry['content_type']))   