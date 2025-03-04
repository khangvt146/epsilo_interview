import yaml
from flask import Flask, jsonify, request

from models.mysql import MySQLConnector
from services.search_vols import SearchVolumeService

with open("config.yml", encoding="utf-8") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

mysql = MySQLConnector(config["MYSQL_CONNECT"])
service = SearchVolumeService(mysql)
app = Flask(__name__)


@app.route("/query", methods=["GET"])
def search_volume():
    """
    API endpoint to execute search volume queries.

    Returns:
        JSON response with success/error message and HTTP status code.
    """
    # Execute query
    result, status_code = service.execute_query_data(request.args)

    if status_code == 200:
        return (
            jsonify(
                {
                    "success": True,
                    "message": "Query executed successfully",
                    "search_volume": result,
                }
            ),
            200,
        )

    elif status_code == 400:
        return (
            jsonify(
                {"success": False, "message": "Validation failed", "errors": result}
            ),
            400,
        )
    elif status_code == 403:
        return (
            jsonify(
                {"success": False, "message": "Unauthorized Users", "errors": result}
            ),
            400,
        )
    else:
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Internal Server Error",
                    "error": result,
                }
            ),
            500,
        )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
