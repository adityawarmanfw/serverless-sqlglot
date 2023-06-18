from flask import Flask, request, jsonify
import json
import sqlglot
from sqlglot import exp


def table_ref(table):
    return "{0}.{1}".format(table.text("db"), table.text("this")) if table.text("db") else table.text("this")


def find_columns(select_statement):
    column_names = []
    for expression in select_statement:
        if isinstance(expression, exp.Alias):
            column_names.append(
                {"name": expression.text("alias"), "sql": expression.sql()})
        elif isinstance(expression, exp.Column):
            column_names.append(
                {"name": expression.text("this"), "sql": expression.sql()})
    return column_names if column_names else [{"name": 'all', "sql": '*'}]


def find_tables(table_statement):
    table_names = []
    if isinstance(table_statement, exp.From):
        for tables in table_statement.find_all(exp.Table):
            table_names.append(table_ref(tables))
    if isinstance(table_statement, list):
        for joins in table_statement:
            for tables in joins.find_all(exp.Table):
                table_names.append(table_ref(tables))
    return table_names


def get_selects(select_stmt, name, kind):
    cols = []
    tables = []

    raw_query = select_stmt.sql()
    for select in select_stmt.find_all(exp.Select):
        sql = select.sql()
        cols = find_columns(select.args['expressions'])
        if 'from' in select.args:
            tables.extend(find_tables(select.args['from']))
        if 'joins' in select.args:
            tables.extend(find_tables(select.args['joins']))

    selects = {
        'id': name,
        'cols': cols,
        'parents': list(set(tables)),
        'sql': raw_query,
        'kind': kind
    }

    return selects


def get_structure(sql):
    ast = sqlglot.parse_one(sql)

    final_model = []

    if 'with' in ast.args:
        for cte in ast.find(exp.With).args['expressions']:
            name = cte.find(exp.TableAlias).text("this")
            final_model.append(get_selects(cte, name, "CTE"))

        final_select = ast.copy()
        final_select.find(exp.With).pop()
    else:
        final_select = ast

    final_model.append(get_selects(final_select, "SELECT", "SELECT"))

    base_tables = []
    for entry in final_model:
        parents = entry.get('parents', [])

        for parent in parents:
            if not any(item['id'] == parent for item in final_model):
                if parent not in base_tables:
                    base_tables.append({
                        'id': parent,
                        'cols': [{'name': 'all', 'sql': '*'}],
                        'parents': [],
                        'sql': f"select * from {parent}",
                        'kind': 'SOURCE'
                    })

    return final_model + base_tables


app = Flask(__name__)


@app.route('/ast')
def parse_to_json():

    # Retrieve the 'sql' query parameter from the URL
    sql = request.args.to_dict().get('sql')
    # Retrieve the 'read' query parameter from the URL
    read = request.args.to_dict().get('read')

    if sql is None:
        return jsonify(error='SQL query is missing'), 400

    try:
        parsed_expressions = [exp.dump() if exp else {} for exp in sqlglot.parse(
            sql, read=read, error_level="ignore")]
        return jsonify(parsed_expressions)
    except Exception as e:
        return jsonify(error=str(e)), 400

@app.route('/cte-tables')
def lineage_to_json():
    # Retrieve the 'sql' query parameter from the URL
    sql = request.args.to_dict().get('sql')

    if sql is None:
        return jsonify(error='SQL query is missing'), 400

    try:
        model = get_structure(sql)
        return jsonify(model)
    except Exception as e:
        return jsonify(error=str(e)), 400
    
@app.route('/transpile')
def transpile():

    # Retrieve the 'sql' query parameter from the URL
    sql = request.args.to_dict().get('sql')
    # Retrieve the 'read' query parameter from the URL
    read = request.args.to_dict().get('read')
    # Retrieve the 'write' query parameter from the URL
    write = request.args.to_dict().get('write')

    if sql is None:
        return jsonify(error='SQL query is missing'), 400

    try:
        parsed_expressions = sqlglot.transpile(sql, read=read, write=write)[0]
        return jsonify(parsed_expressions)
    except Exception as e:
        return jsonify(error=str(e)), 400
