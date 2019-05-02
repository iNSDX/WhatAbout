from flask import Flask,jsonify,request
from flask import render_template
import ast

app = Flask(__name__)
tweets = []
scores = []

@app.route("/")
def get_home_page():
    global tweets,scores
    tweets = []
    scores = []
    return render_template('tweets.html', tweets=tweets, scores=scores)

@app.route('/refreshData')
def refresh_tweets():
    global tweets,scores
    print("Number of tweets now: " + str(len(tweets)))
    return jsonify(sTweets=tweets, sScores=scores)

@app.route('/updateData', methods=['POST'])
def update_data():
    global tweets,scores
    if not request.form or 'tweets' not in request.form or 'scores' not in request.form:
        return "error",400
    tweets = ast.literal_eval(request.form['tweets'])
    scores = ast.literal_eval(request.form['scores'])

    print("Number of tweets received: " + str(len(tweets)))
    return "success", 201

if __name__ == "__main__":
    app.run(host='localhost', port=5000)