import logging
from locust import task, run_single_user
from chroma_user import ChromaUser,CHROMA_HOST
from asset_transformer import get_modified_assets
from assets_dhh import DHH_ASSETS

logging.basicConfig(level=logging.INFO)

class ChromaTestUser(ChromaUser):
    host = CHROMA_HOST

    @task
    def test_find_similar_environment_entities(self):
        collection_name = "dhh"
        entities_to_query = get_modified_assets(DHH_ASSETS, 3)
        max_distance = 0.3
        n_results = 3
        try:
            collection = self.client.get_collection(collection_name)
            if collection:
                similar_entities = list(
                    self.client.find_similar_environment_entities(
                        collection=collection,
                        entities_to_query=entities_to_query,
                        max_distance=max_distance,
                        n_results=n_results
                    )
                )
                logging.info(f"Similar entities: {similar_entities}")
            else:
                logging.warning(f"Collection '{collection_name}' not found.")
        except Exception as e:
            logging.error(f"Error in test_find_similar_environment_entities: {e}")

    # @task
    # def list_collections(self):
    #     collections = list(self.client.list_collections())
    #     logging.info("Collections:")
    #     for collection in collections:
    #         logging.info(f"- {collection.name}")

if __name__ == "__main__":
    run_single_user(ChromaTestUser)
