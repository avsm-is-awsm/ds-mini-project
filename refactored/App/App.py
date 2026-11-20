from flask import Flask
from Classes.Nodes.LeadNode import LeadNode

def create_app(lead_node: LeadNode):
    """
    Create a Flask application using 1 lead node to manage the storage system

    :param LeadNode lead_node: The lead node of the distributed storage system
    """

    app = Flask(__name__)

    app.config["LEAD_NODE"] = lead_node

    from App.Routes import main
    app.register_blueprint(main)

    return app