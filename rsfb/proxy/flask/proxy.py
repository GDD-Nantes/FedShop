import re
import shutil
import sys
import tempfile
from urllib.parse import urlparse
import requests
from flask import Flask, Response, request, stream_with_context
from flask_cors import CORS, cross_origin

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

HTTP_COUNT = 0
DATA_TRANSFER = 0
HTTP_ASK = 0

def is_running_in_docker():
    """
    Checks if the code is running inside a Docker container.
    """
    try:
        with open('/proc/1/cgroup', 'rt') as f:
            return 'docker' in f.read()
    except IOError:
        return False

def calculate_data_transfer_size(response: requests.Response):
    if 'content-length' not in response.headers:        
        # stream content into a temporary file so we can get the real size
        spool = tempfile.SpooledTemporaryFile(2**20)
        shutil.copyfileobj(response.raw, spool)
        response.headers['content-length'] = str(spool.tell())
        spool.seek(0)
        # replace the original socket with our temporary file
        response.raw._fp.close()
        response.raw._fp = spool
    return int(response.headers['content-length'])

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST'])
@app.route('/<path:path>', methods=['GET', 'POST'])
def proxy(path):
    
    # Skip proxy's own methods:
    if path == "total_requests":
        return get_total_requests()
    elif path == "total_data_transfer":
        return get_total_size()
    elif path == "reset":
        return reset_statistics()
    elif path == "set_destination":
        return set_destination()
        
    # Access to global variables
    global HTTP_COUNT
    global DATA_TRANSFER
    global VIRTUOSO_URL
    global HTTP_ASK
    
    # Forward the request to the target server        
    target_url = urlparse(VIRTUOSO_URL).geturl() + path  # Replace with your target server URL    
    params = {k: v for k, v in request.args.items()}
    forms = {k: v for k, v in request.form.items()}   
    headers = {key: value for (key, value) in request.headers if key != 'Host'} 
    response = requests.request(request.method, target_url, headers=headers, params=params, data=forms, stream=True)
    
    # Get the response's data transfer size
    data_transfer_size = calculate_data_transfer_size(response)
    
    HTTP_COUNT += 1
    DATA_TRANSFER += data_transfer_size
    
    if params.get("query") is not None:
        query = params.get("query").lower()
        if re.search(r".*select.*limit 1", query) is not None:
            HTTP_ASK += 1

    return Response(stream_with_context(response.iter_content()), content_type=response.headers['content-type'])


@app.route('/total_data_transfer')
def get_total_size():
    global DATA_TRANSFER  # Access the global variable
    return {"total_data_transfer": DATA_TRANSFER}

@app.route('/total_request')
def get_total_requests():
    global HTTP_COUNT  # Access the global variable
    return {"total_http_request": HTTP_COUNT}

@app.route('/total_ask')
def get_total_ask():
    global HTTP_ASK  # Access the global variable
    return {"total_ask": HTTP_ASK}

@app.route('/set_destination', methods=['POST'])
def set_destination():
    global VIRTUOSO_URL

    VIRTUOSO_URL = request.form.get('destination')
    if is_running_in_docker():
        port = re.search(r":(\d+)", VIRTUOSO_URL).group(1)
        VIRTUOSO_URL = f"http://host.docker.internal:{port}/"

    return f"Destination address ({VIRTUOSO_URL}) set successfully."

@app.route('/reset')
def reset_statistics():
    global HTTP_COUNT
    global DATA_TRANSFER 
    global HTTP_ASK
    
    HTTP_COUNT = 0
    DATA_TRANSFER = 0
    HTTP_ASK = 0
    
    return "RESET_OK"

if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True)
