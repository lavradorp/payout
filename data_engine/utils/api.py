import requests

class API:
    def __init__(self, base_url):
        self.base_url = base_url

    def request(self, method, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"
        
        response = requests.request(method, url, params=params, timeout=5)
        response.raise_for_status()

        return response.json()
