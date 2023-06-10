from flask import Flask, request, jsonify
import json
import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType

app = Flask(__name__)

@app.route('/')
def parse_to_json():

    sql = request.args.to_dict().get('sql')  # Retrieve the 'sql' query parameter from the URL
    read = request.args.to_dict().get('read')  # Retrieve the 'read' query parameter from the URL

    if sql is None:
        return jsonify(error='SQL query is missing'), 400

    try:
        parsed_expressions = [exp.dump() if exp else {} for exp in sqlglot.parse(sql, read=read, error_level="ignore")]
        return jsonify(parsed_expressions)
    except Exception as e:
        return jsonify(error=str(e)), 400