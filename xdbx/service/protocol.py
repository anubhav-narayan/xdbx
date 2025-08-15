import json

def parse_request(data):
    return json.loads(data.decode('utf-8'))

def format_response(status, message=None, value=None):
    return json.dumps({
        "status": status,
        "message": message,
        "value": value
    }).encode('utf-8')
