import json

from flask import Flask, request, Response
from webhook_parser import usecase

app = Flask(__name__)


@app.route("/v1/<topic>", methods=['GET', 'POST'])
def parser(topic):
    if request.headers.get("Content-Type") == "application/json":
        usecase.store_to_kafka(topic, request.json)
    else:
        data = request.form.to_dict()
        for key, value in data.items():
            try:
                data[key] = json.loads(value)
            except:
                data[key] = value

        usecase.store_to_kafka(topic, data)
    return "ok"


@app.route("/v1/<topic>/<field>", methods=['GET', 'POST'])
def parser_multiple(topic, field):
    if request.headers.get("Content-Type") == "application/json":
        data = request.json
    else:
        data = request.form.to_dict()
        for key, value in data.items():
            try:
                data[key] = json.loads(value)
            except:
                data[key] = value
    result = usecase.process_multiple_rows(topic, data, field)

    if result == "ok":
        return Response(result, status=200)
    else:
        return Response(result.format(result), status=400)
