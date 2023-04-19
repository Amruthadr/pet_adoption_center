import json
import random
import datetime
import time
from typing import List, Dict

from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["pet_adoption"]  # Replace "your_database_name" with the desired name of your MongoDB database


def clear_database():
    db.address.drop()
    db.owner.drop()
    db.dog.drop()
    db.adoption.drop()
    print("Cleared the database.")


def get_breed_documents(dog_breeds: List[Dict]):
    breed_documents = []

    for dog_breed in dog_breeds:
        breed_document = {
            "name": dog_breed["Name"],
            "description": "\n".join(dog_breed["Description"]).replace("'", ""),
            "profile_url": dog_breed["ProfileUrl"],
            "breed_characteristics": dog_breed["BreedCharacteristics"],
            "vital_stats": dog_breed["VitalStats"],
            "more_about": dog_breed["MoreAbout"],
            "images_urls": dog_breed["ImagesUrls"],
        }
        breed_documents.append(breed_document)
    return breed_documents


def insert_addresses(owners: List[Dict]):
    collection = db["address"]
    documents = []

    for owner in owners:
        document = {
            "address": owner["Address"]["Address"],
            "city": owner["Address"]["City"],
            "country": owner["Address"]["Country"],
            "zip": owner["Address"]["Zip"]
        }
        documents.append(document)

    result = collection.insert_many(documents)
    print(f"Inserted {len(result.inserted_ids)} rows to address collection.")
    return result


def insert_owners(owners: List[Dict], address_ids: List):
    collection = db["owner"]
    documents = []

    for idx, owner in enumerate(owners):
        document = {
            "full_name": owner["Name"],
            "email_id": owner["Email"],
            "mobile": owner["Mobile"],
            "address_id": address_ids[idx]
        }
        documents.append(document)

    result = collection.insert_many(documents)
    print(f"Inserted {len(result.inserted_ids)} rows to owner collection.")
    return result


def random_past_date(past_years: int = 5) -> datetime.datetime:
    today = datetime.datetime.now()
    past_date = today - datetime.timedelta(days=past_years * 365)
    days_between = (today - past_date).days
    random_days = random.randint(0, days_between)
    random_datetime = past_date + datetime.timedelta(days=random_days, hours=random.randint(0, 23),
                                                     minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    return random_datetime


def insert_adoption(owner_ids_collection: List[object], dog_ids_collection: List[object], ignore_frequency: int = 10):
    collection = db["adoption"]
    dogs_count = len(dog_ids_collection)
    adoption_list = []

    adoption_id_results = []
    for i in range(1, dogs_count + 1):
        if i % ignore_frequency == 0:
            continue

        adoption_date = random_past_date()
        adoption_record = {
            "dog_id": dog_ids_collection[i - 1],
            "owner_id": random.choice(owner_ids_collection),
            "adoption_date": adoption_date
        }
        adoption_list.append(adoption_record)

        if len(adoption_list) % 25000 == 0:
            result = collection.insert_many(adoption_list)
            print(f"Inserted {len(result.inserted_ids)} rows to adoption collection.")
            adoption_id_results += result.inserted_ids
            adoption_list = []

    if adoption_list:
        result = collection.insert_many(adoption_list)
        print(f"Inserted {len(result.inserted_ids)} rows to adoption collection.")
        adoption_id_results += result.inserted_ids

    return adoption_id_results


def get_owners() -> List[Dict]:
    with open("./data/owners.json", "r", encoding="utf-8") as file:
        return json.load(file)


def get_dog_data() -> str:
    with open("./data/dog_breed_data.json", "r", encoding="utf-8") as file:
        return json.load(file)


def get_owner_data() -> str:
    with open("./data/owner_data.json", "r", encoding="utf-8") as file:
        return json.load(file)


def get_random_names() -> List[str]:
    with open("./data/random_names.json", "r", encoding="utf-8") as file:
        return json.load(file)


def insert_dog(dog_names_collections: List[str], breed_info_collections: List[object],
               address_id_collections: List[str],
               max_entry: int = 1000):
    collection = db["dog"]
    dog_documents = []
    num_entries = 0
    dog_id_results = []
    for _ in range(max_entry):
        dog = {
            "breed": random.choice(breed_info_collections),
            "name": random.choice(dog_names_collections),
            "address_id": random.choice(address_id_collections)
        }
        dog_documents.append(dog)
        num_entries += 1
        if num_entries % 50000 == 0:
            result = collection.insert_many(dog_documents)
            print(f"Inserted {len(result.inserted_ids)} rows to dog collection.")
            dog_id_results += result.inserted_ids
            dog_documents = []

    if dog_documents:
        result = collection.insert_many(dog_documents)
        print(f"Inserted {len(result.inserted_ids)} rows to dog collection.")
        dog_id_results += result.inserted_ids

    return dog_id_results


def print_db_size():
    stats = db.command("dbStats")
    storage_size_bytes = stats["storageSize"]
    storage_size_mb = storage_size_bytes / (1024 * 1024)
    storage_size_gb = storage_size_mb / 1024
    print(f"Database size: {storage_size_mb:.2f} MB ({storage_size_gb:.2f} GB)")


if __name__ == "__main__":
    print_db_size()

    clear_database()

    dog_breed_data = get_dog_data()
    breed_documents = get_breed_documents(dog_breed_data)

    owner_data = get_owner_data()
    address_insertion_result = insert_addresses(owner_data)
    address_ids = address_insertion_result.inserted_ids
    owner_insertion_result = insert_owners(owner_data, address_ids)

    random_names = get_random_names()
    max_entries = 250000
    dog_inserted_ids = insert_dog(random_names, breed_documents, address_ids, max_entry=max_entries)

    adoption_insertion_result = insert_adoption(owner_insertion_result.inserted_ids, dog_inserted_ids,
                                                ignore_frequency=10)
    print("Data loading successful.")
    print_db_size()
