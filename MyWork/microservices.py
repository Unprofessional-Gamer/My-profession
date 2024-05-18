#!/usr/bin/env python

from flask import Flask, request,Response
import pandas as pd
import subprocess

app = Flask(__name__)

# intialize gcs and BQ client
project_id = "tnt01-odycda-bld-01-1b81"

@app.route('/')
def hello_world():
    return 'Hello, World!'

#--sompte snippet to replicate
@app.route('/check', methods=['GET']) #--routing name /Dim_Product to be changed of your choice
def check(): # ---change the function name
    script_path = '/home/appuser/GFV/scripts/cert_checks.py' #--Location of the script
    try:
        result = subprocess.check_output(['python', script_path], stderr=subprocess.STDOUT, text=True)
        return Response(f'test run.output: {result}',200)
    except subprocess.CalledProcessError as e:
        return Response (f'execution error test: {e.output})')


#-------sample snippet to replicate
@app.route('/baseresidualload', methods=['GET'])
def base_residual_values_load():
    script_path = '/home/appuser/Spanner_test/rv_mapping_ids.py'
    setup_file='/home/appuser/Spanner_test/setup.py'
    try:
        result = subprocess.check_output(['python', script_path, f'--setup={setup_file}'],stderr=subprocess.STDOUT, text=True)
        return f'dataflow run.output: (result)'
    except subprocess.CalledProcessError as e:
        return f'execution error: (e.output)'


# ---sample snippet to replicate ---end
if __name__ == '__main__':
    app.run(host='0.0.0.0',port=9080)
