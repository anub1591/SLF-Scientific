
import flask_restful
from flask import Flask,request
from flask_restful import Resource, Api
import pickle
import pandas as pd
from flask_cors import CORS

# creating an instance of flask and saving it as app
app = Flask(__name__)

# calling the CORS for cross origin resource sharing and supply it to the app
CORS(app)

# creating an API object
# creating an instance for our app which would help us in url routing & to this object we will pass the app instamce flask
api = Api(app)

# prediction api
# to get the model prediction lets define a prediction class
class prediction(Resource):
    def get(self):
        classifier = pickle.load(open('model.pkl', 'rb'))
        return classifier


# data api
class getData(Resource):
    def get(self):
        df = pd.read_csv('wine.data.txt')
        res = df.to_json (orient='records')
        return res

#creating an end point
api.add_resource(getData, '/data')
api.add_resource(prediction, '/prediction')


if __name__ = '__main__':
    app.run(debug=True)
