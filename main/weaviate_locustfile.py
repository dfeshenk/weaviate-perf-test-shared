from gevent import monkey
monkey.patch_all()
for mod in ['socket', 'threading', 'ssl', 'time']:
    print(f"Locustfile {mod}: patched =", monkey.is_module_patched(mod))

import logging
from locust import task, run_single_user, constant_throughput, runners, events
from weaviate_user import WeaviateUser, HTTP_HOST
from asset_transformer import get_modified_assets
from assets_dhh import DHH_ASSETS
import traceback
import time, sys

logging.basicConfig(level=logging.INFO)

runners.HEARTBEAT_DEAD_INTERNAL = 600
runners.HEARTBEAT_LIVENESS = 300
runners.CONNECT_TIMEOUT = 60

class WeaviateTestUser(WeaviateUser):
    host = HTTP_HOST

    @task
    def test_find_similar_environment_entities(self):
        collection_name = "dhh"
        entities_to_query = get_modified_assets(DHH_ASSETS, 3)
        max_distance = 0.3
        n_results = 3
        start_time = time.time()
        try:
            collection = self.client.get_collection(collection_name)
            if collection is not None:
                similar_entities = self.client.find_similar_environment_entities(
                    collection=collection,
                    entities_to_query=entities_to_query,
                    max_distance=max_distance,
                    n_results=n_results
                )
                if similar_entities and len(similar_entities) > 0:
                    documents = [obj.properties.get("document") for obj in similar_entities[0].objects]
                else:
                    documents = []
                    logging.warning("No similar entities found.")
                logging.info(f"Query: {entities_to_query} ; Similar: {documents}")
            else:
                logging.warning(f"Collection '{collection_name}' not found.")
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="weaviate",
                name="total_find_similar_environment_entities",
                response_time=total_time,
                response_length=0,
                response=None,
                context={},
                exception=None
            )
        except:
            total_time = int((time.time() - start_time) * 1000)
            logging.error(traceback.format_exc())
            events.request.fire(
                request_type="weaviate",
                name="total_find_similar_environment_entities",
                response_time=total_time,
                response_length=0,
                response=None,
                context={},
                exception=f"Error: {sys.exc_info()[0]}"
            )

    wait_time = constant_throughput(1)

    # @task
    # def get_asset_names(self):
    #     objectNames = self.client.get_asset_names(collection_name="Guardicore_59742320")
    #     logging.info(f"Asset Names: {objectNames}")

    # @task
    # def list_collections(self):
    #     collections = self.client.list_collections()
    #     logging.info(f"Collections: {collections}")

    # @task
    # def get_object_count(self):
    #     collections = list(self.client.list_collections())
    #     if collections:
    #         for collection_name in collections:
    #             count = self.client.get_object_count(collection_name)
    #             logging.info(f"{collection_name}: {count} objects")


# if __name__ == "__main__":
#     run_single_user(WeaviateTestUser)