import logging

from locust import task, run_single_user
from weaviate_user import WeaviateUser, HTTP_HOST
from asset_transformer import get_modified_assets
from assets_dhh import DHH_ASSETS

logging.basicConfig(level=logging.INFO)

class WeaviateTestUser(WeaviateUser):
    host = HTTP_HOST
    
    @task
    def test_find_similar_environment_entities(self):
        collection_name = "dhh"
        entities_to_query = get_modified_assets(DHH_ASSETS, 3)
        max_distance = 0.3
        n_results = 3
        try:
            collection = self.client.get_collection(collection_name)
            if collection:
                similar_entities = self.client.find_similar_environment_entities(
                        collection=collection,
                        entities_to_query=entities_to_query,
                        max_distance=max_distance,
                        n_results=n_results
                    )
                documents = [obj.properties.get("document") for obj in similar_entities[0].objects]
                logging.info(f"Query: {entities_to_query} ; Similar: {documents}")
            else:
                logging.warning(f"Collection '{collection_name}' not found.")
        except Exception as e:
            logging.error(f"Error in test_find_similar_environment_entities: {e}")


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

    
if __name__ == "__main__":
    run_single_user(WeaviateTestUser)