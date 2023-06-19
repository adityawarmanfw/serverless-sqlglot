from flask import Flask, request, jsonify
import json
import sqlglot
from sqlglot import exp


def table_ref(table):
    return "{0}.{1}".format(table.text("db"), table.text("this")) if table.text("db") else table.text("this")

def get_columns_lineage(column):
    if "table" in column.args:
        parent_table = column.args["table"].text("this")
    else:
        parent_table = "LITERAL"
    parent_column = column.text("this")
    lineage = {"table": parent_table, "col": parent_column}
    return lineage

def get_columns(select_statement):
    column_names = []
    for expression in select_statement:
        if isinstance(expression, exp.Alias):
            column_parents = []

            for column in expression.find_all(exp.Column):
                column_parents.append(get_columns_lineage(column))

            for literal in expression.find_all(exp.Literal):              
                column_parents.append(get_columns_lineage(literal))
            
            column_names.append({
                "col": expression.text("alias"), 
                "parents": list(column_parents), 
                "sql": expression.sql()
                })
                
    return column_names if column_names else [{"col": "STAR", "parents": [], "sql": "*"}]


def get_tables(table_statement):
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
        cols = get_columns(select.args["expressions"])
        if "from" in select.args:
            tables.extend(get_tables(select.args["from"]))
        if "joins" in select.args:
            tables.extend(get_tables(select.args["joins"]))

    selects = {
        "table": name,
        "kind": kind,
        "cols": cols,
        "table_parents": list(set(tables)),
        "table_sql": raw_query,
    }

    return selects


def get_lineage(sql):
    ast = qualify_columns(sqlglot.parse_one(sql), schema=None)

    final_model = []

    if "with" in ast.args:
        for cte in ast.find(exp.With).args["expressions"]:
            name = cte.find(exp.TableAlias).text("this")
            final_model.append(get_selects(cte, name, "CTE"))

        final_select = ast.copy()
        final_select.find(exp.With).pop()
    else:
        final_select = ast

    final_model.append(get_selects(final_select, "SELECT", "SELECT"))

    base_tables = []
    for entry in final_model:
        parents = entry.get("parents", [])

        for parent in parents:
            if not any(item['table'] == parent for item in final_model):
                if parent not in base_tables:
                    base_tables.append({
                        "table": parent,
                        "kind": "SOURCE",
                        "cols": [{"name": "STAR", "parents": [], "sql": "*"}],
                        "table_parents": [],
                        "table_sql": f"SELECT * FROM {parent}",
                    })

    return final_model + base_tables


app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True

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

@app.route('/lineage')
def lineage_to_json():
    # Retrieve the 'sql' query parameter from the URL
    sql = request.args.to_dict().get('sql')

    if sql is None:
        return jsonify(error='SQL query is missing'), 400

    try:
        model = get_lineage(sql)
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
