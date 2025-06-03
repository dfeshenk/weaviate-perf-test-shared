import time
import os
import logging
import weaviate
import traceback

from locust import User,events
from weaviate.config import AdditionalConfig, Timeout
from weaviate.classes.init import Auth
from weaviate.classes.query import Filter

logging.basicConfig(level=logging.INFO)

HTTP_HOST = os.getenv("WEAVIATE_HTTP_HOST", "localhost")
HTTP_PORT = os.getenv("WEAVIATE_HTTP_PORT", 8080)
HTTP_SECURE=bool(int(os.getenv("WEAVIATE_HTTP_SECURE", 0)))
GRPC_HOST = os.getenv("WEAVIATE_GRPC_HOST", "localhost")
GRPC_PORT = int(os.getenv("WEAVIATE_GRPC_PORT", 50051))
GRPC_SECURE=bool(int(os.getenv("WEAVIATE_GRPC_SECURE", 0)))
AZURE_OPENAI_API_KEY = os.getenv('AZURE_OPENAI_API_KEY')
WEAVIATE_API_KEY = os.getenv('WEAVIATE_API_KEY')

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    logging.info(f"-----------------------------------------------------------------------")
    logging.info(f"WEAVIATE_HTTP_HOST: {HTTP_HOST}")
    logging.info(f"WEAVIATE_HTTP_PORT: {HTTP_PORT}")
    logging.info(f"WEAVIATE_HTTP_SECURE: {HTTP_SECURE}")
    logging.info(f"WEAVIATE_GRPC_HOST: {GRPC_HOST}")
    logging.info(f"WEAVIATE_GRPC_PORT: {GRPC_PORT}")
    logging.info(f"WEAVIATE_GRPC_SECURE: {GRPC_SECURE}")
    logging.info(f"-----------------------------------------------------------------------")

class WeaviateClient:
    def __init__(self, host, request_event):
        self.host = host
        self._request_event = request_event
        self.client = None
        self.connect()

    def connect(self):
        try:
            self.client = weaviate.connect_to_custom(
                http_host=HTTP_HOST,
                http_port=HTTP_PORT,
                http_secure=HTTP_SECURE,
                grpc_host=GRPC_HOST,
                grpc_port=GRPC_PORT,
                grpc_secure=GRPC_SECURE,
                headers = {"X-Azure-Api-Key": AZURE_OPENAI_API_KEY},
                auth_credentials=Auth.api_key(os.getenv("WEAVIATE_API_KEY", WEAVIATE_API_KEY)),
                additional_config=AdditionalConfig(
                    timeout=Timeout(
                        init=int(os.getenv("WEAVIATE_TIMEOUT_INIT", 30)),
                        query=int(os.getenv("WEAVIATE_TIMEOUT_QUERY", 60)),
                        insert=int(os.getenv("WEAVIATE_TIMEOUT_INSERT", 120))
                    )
                )            
            )
            if not self.client.is_ready():
                raise RuntimeError(
                    f"Weaviate client is not ready."
                )
        except Exception as e:
            logging.error(f"Failed to connect to Weaviate: {e}")
        except:
            logging.error(traceback.format_exc())

    def request(self, name, func, *args, **kwargs):
        request_meta = {
            "response_length": 0,
            "request_type": "weaviate",
            "name": name,
            "start_time": time.time(),
            "response": None,
            "context": {},
            "exception": None
        }
        start_perf_counter = time.perf_counter()
        try:
            request_meta["response"] = func(*args, **kwargs)
        except Exception as e:
            request_meta["exception"] = e
        except:
            logging.error(traceback.format_exc())
        finally:
            request_meta["response_time"] = (time.perf_counter() - start_perf_counter) * 1000
            self._request_event.fire(**request_meta)
        return request_meta["response"]

    def list_collections(self):
        return self.request("list_collections", self.client.collections.list_all)
    
    def get_asset_names(self, collection_name):
        collection = self.client.collections.get(collection_name)
        response = collection.query.fetch_objects(
            filters=Filter.by_property("meta_object_type").equal("assets"),
            limit=10000
        )
        return [obj.properties.get("document") for obj in response.objects]

    def get_object_count(self, collection_name):
        return self.request(f"get_object_count_{collection_name}", lambda: self.client.collections.get(collection_name).aggregate.over_all(total_count=True).total_count)
    
    def get_collection(self, collection_name):
        return self.request(f"get_collection_{collection_name}", self.client.collections.get, collection_name)

    def find_similar_environment_entities(self, collection, entities_to_query: list, max_distance: float = 0.3,
                                          n_results: int = 3, output_chromadb_compatible=False) -> list[dict]:
        def _search():
            entities_lower = [s.lower() for s in entities_to_query]
            limit = n_results * len(entities_lower)
            results = []
            if entities_lower:
                response = collection.query.near_text(
                    query=entities_lower,
                    distance=max_distance,
                    limit=limit
                )
                if output_chromadb_compatible:
                    for obj in response.objects:
                        obj_res = {
                            "id": obj.properties.id,
                            "document": obj.properties.document,
                            "distance": obj.metadata.distance,
                            "metadata": obj.properties.metadata
                        }
                        results.append(obj_res)
                else:
                    results.append(response)
            return results
        return self.request("find_similar_environment_entities", _search)

    def close(self):
        if self.client:
            self.client.close()
            logging.info("Connection to Weaviate closed.")

class WeaviateUser(User):
    abstract = True

    def __init__(self, environment):
        super().__init__(environment)
        self.client = WeaviateClient(self.host, request_event=environment.events.request)

    def on_start(self) -> None:
        logging.info(f"host: {self.client.host}")

    def on_stop(self) -> None:
        self.client.close()

