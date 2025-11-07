import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

#load variables from .env
load_dotenv()

def get_blob_service():
	conn = os.getenv("AZ_STORAGE_CONNECTION_STRING")
	if not conn:
		raise RuntimeError("AZ_STORAGE_CONNECTION_STRING is missing in .env")
	return BlobServiceClient.from_connection_string(conn)

if __name__ == "__main__":
	svc = get_blob_service()
	containers = [c['name'] if isinstance(c, dict) else c.name for c in svc.list_containers()]
	print("Containers:", containers)
