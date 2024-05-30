from locust import HttpUser, task, between

class MyUser(HttpUser):
    wait_time = between(5, 9)  # simulated wait time between tasks

    @task
    def query_api(self):
        url = "/v1/query"
        payload = {
            "query": "get_all_fuel",
            "params": {"limit": "3"}
        }
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "insomnia/9.2.0"
        }
        self.client.post(url, json=payload, headers=headers)
