
import os

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi

load_dotenv(override=True)

required = [
    "MONGODB_URI",
    "MONGODB_USERNAME",
    "MONGODB_PASSWORD"
]

for name in required:
    if not os.getenv(name):
        raise ValueError(f"Missing environment variable: {name}")


client = None

try:
    client = MongoClient(
        os.environ["MONGODB_URI"],
        username=os.environ["MONGODB_USERNAME"],
        password=os.environ["MONGODB_PASSWORD"],
        authSource="admin",
        server_api=ServerApi("1"),
        serverSelectionTimeoutMS=15000,
        connectTimeoutMS=10000,
        appname="BanglaPay-Python-Test"
    )

    result = client.admin.command("ping")

    database = client[
        os.getenv("MONGODB_DATABASE", "banglapay_ops")
    ]

    print("PASS: MongoDB Python connection")
    print("Ping:", result)
    print("Database:", database.name)
    print("Collections:", database.list_collection_names())

except Exception as exc:
    print("FAIL:", type(exc).__name__)
    print(str(exc))

finally:
    if client:
        client.close()
