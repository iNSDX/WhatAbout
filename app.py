from flask import Flask,jsonify,request
from flask import render_template
import ast

app = Flask(__name__)
labels = []
values = []
sw = "" # Search term used

@app.route("/")
def get_chart_page():
    global labels,values,sw
    labels = []
    values = []
    sw = ""
    return render_template('chart.html', values=values, labels=labels, sw=sw)

@app.route('/refreshData')
def refresh_graph_data():
    global labels, values,sw
    print("labels now: " + str(labels))
    print("data now: " + str(values))
    print("search term now: " + str(sw))
    return jsonify(sLabel=labels, sData=values, sSw=sw)

@app.route('/updateData', methods=['POST'])
def update_data():
    global labels, values,sw
    if not request.form or 'data' not in request.form:
        return "error",400
    labels = ast.literal_eval(request.form['label'])
    values = ast.literal_eval(request.form['data'])
    # sw = ast.literal_eval(request.form['sw'])
    print("labels received: " + str(labels))
    print("data received: " + str(values))
    print("search term: " + str(sw))
    return "success", 201

if __name__ == "__main__":
    app.run(host='localhost', port=5001)