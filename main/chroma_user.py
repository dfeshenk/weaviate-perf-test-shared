import os
import time
import logging
import chromadb
import traceback
from chromadb.config import Settings
from locust import User, events
from chromadb.utils import embedding_functions

logging.basicConfig(level=logging.INFO)

# Read environment variables
CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", 8000))
CHROMA_USER = os.getenv("CHROMA_USER", "chroma")
CHROMA_PASSWORD = os.getenv("CHROMA_PASSWORD", "")
CHROMA_USE_SSL = bool(int(os.getenv("CHROMA_USE_SSL", 0)))
AZURE_OPENAI_API_KEY = os.getenv('AZURE_OPENAI_API_KEY')

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    logging.info("-----------------------------------------------------------------------")
    logging.info(f"CHROMA_HOST: {CHROMA_HOST}")
    logging.info(f"CHROMA_PORT: {CHROMA_PORT}")
    logging.info(f"CHROMA_USE_SSL: {CHROMA_USE_SSL}")
    logging.info("-----------------------------------------------------------------------")

class ChromaClient:
    def __init__(self, request_event):
        self._request_event = request_event
        self.client = None
        self.connect()

    def connect(self):
        try:
            settings = Settings(
                chroma_client_auth_provider="chromadb.auth.basic_authn.BasicAuthClientProvider",
                chroma_client_auth_credentials=f"{CHROMA_USER}:{CHROMA_PASSWORD}",
                anonymized_telemetry=False
            )
            self.client = chromadb.HttpClient(
                host=CHROMA_HOST,
                port=CHROMA_PORT,
                ssl=CHROMA_USE_SSL,
                settings=settings
            )
            # Perform a simple operation to verify the connection
            self.client.heartbeat()
            logging.info("Successfully connected to ChromaDB.")
        except Exception as e:
            logging.error(f"Failed to connect to ChromaDB: {e}")
        except:
            logging.error(traceback.format_exc())

    def request(self, name, func, *args, **kwargs):
        request_meta = {
            "response_length": 0,
            "request_type": "chroma",
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
            logging.error(f"Error in {name}: {e}")
        except:
            logging.error(traceback.format_exc())
        request_meta["response_time"] = (time.perf_counter() - start_perf_counter) * 1000
        self._request_event.fire(**request_meta)
        return request_meta["response"]

    def list_collections(self):
        return self.request("list_collections", self.client.list_collections)

    def get_collection(self, collection_name):
        openai_ef = embedding_functions.OpenAIEmbeddingFunction(
            model_name="text-embedding-ada-002",
            api_key=AZURE_OPENAI_API_KEY,
            api_type="azure",
            api_version="2024-02-15-preview",
            api_base="https://networklogscopilot.openai.azure.com/"
        )
        
        return self.request(f"get_collection_{collection_name}", self.client.get_collection, collection_name, embedding_function=openai_ef)

    def find_similar_environment_entities(self, collection, entities_to_query: list, max_distance: float = 0.3,
                                          n_results: int = 3) -> list[dict]:
        def _search():
            if entities_to_query:
                env_entity_search_results = collection.query(
                    query_texts=entities_to_query,
                    where={"object_type": "assets"},
                    n_results=n_results
                )
                # Flatten and filter results based on max_distance
                filtered_results = [
                    {'id': id, 'document': doc, 'distance': dist, 'metadata': meta}
                    for id_group, distance_group, document_group, meta_group in zip(
                        env_entity_search_results['ids'],
                        env_entity_search_results['distances'],
                        env_entity_search_results['documents'],
                        env_entity_search_results['metadatas']
                    )
                    for id, dist, doc, meta in zip(id_group, distance_group, document_group, meta_group)
                    if dist <= max_distance
                ]
            else:
                filtered_results = []
            return filtered_results

        return self.request("find_similar_environment_entities", _search)

    def close(self):
        # If the client has a close method, call it
        if hasattr(self.client, "close"):
            self.client.close()
            logging.info("Connection to ChromaDB closed.")

class ChromaUser(User):
    abstract = True

    def __init__(self, environment):
        super().__init__(environment)
        self.client = ChromaClient(request_event=environment.events.request)

    def on_start(self):
        logging.info("ChromaUser test started.")

    def on_stop(self):
        self.client.close()
