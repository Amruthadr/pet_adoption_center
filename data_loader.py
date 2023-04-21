import json
import random
import datetime
from typing import List, Dict

from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["pet_adoption"]  # Replace "your_database_name" with the desired name of your MongoDB database


def clear_database():
    db.owner.drop()
    db.dog.drop()
    db.adoption.drop()
    print("Cleared the database.")


def get_breed_documents(dog_breeds: List[Dict]):
    breeds = []
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
        breeds.append(breed_document)
    return breeds


def insert_owners(owners):
    collection = db["owner"]
    batch_size = 25000

    owner_id_results = []

    for i in range(0, len(owners), batch_size):
        batch = owners[i:i + batch_size]
        result = collection.insert_many(batch)
        owner_id_results += result.inserted_ids
        print(f"Inserted {len(result.inserted_ids)} rows to owner collection (Batch {i // batch_size + 1}).")

    print(f"Inserted {len(owner_id_results)} rows to owner collection in total.")
    return owner_id_results


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


def generate_dummy_owners(names: list, addresses: list, cities: list, countries: list, num_owners: int):
    dummy_owners = []

    for _ in range(num_owners):
        first_name = random.choice(names)
        last_name = random.choice(names)
        name = f"{first_name} {last_name}"
        email = f"{first_name.lower()}.{last_name.lower()}@puppyworld.in"
        mobile = ''.join([str(random.randint(0, 9)) for _ in range(10)])

        address = random.choice(addresses)
        shuffled_address = ' '.join(random.sample(address.split(), len(address.split())))

        city = random.choice(cities)
        country = random.choice(countries)
        zip_code = ''.join([str(random.randint(0, 9)) for _ in range(5)])

        owner = {
            "name": name,
            "email": email,
            "mobile": mobile,
            "address": {
                "street": shuffled_address,
                "city": city,
                "country": country,
                "zip": zip_code
            }
        }
        dummy_owners.append(owner)

    return dummy_owners


def insert_dog(dog_names_collections: List[str], breed_info_collections: List[object],
               countries_collections: List[str],
               max_entry: int = 1000):
    collection = db["dog"]
    dog_documents = []
    num_entries = 0
    dog_id_results = []
    for _ in range(max_entry):
        dog = {
            "breed": random.choice(breed_info_collections),
            "name": random.choice(dog_names_collections),
            "country": random.choice(countries_collections)
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
    unique_names = get_random_names()
    unique_countries = list(set([owner["Address"]["Country"] for owner in owner_data]))
    unique_cities = list(set([owner["Address"]["City"] for owner in owner_data]))
    unique_addresses = list(set([owner["Address"]["Address"] for owner in owner_data]))
    max_entries = 250000

    unique_owners = generate_dummy_owners(unique_names, unique_addresses, unique_cities, unique_countries, max_entries)
    owner_ids = insert_owners(unique_owners)

    dog_inserted_ids = insert_dog(unique_names, breed_documents, unique_countries, max_entry=max_entries)

    adoption_insertion_result = insert_adoption(owner_ids, dog_inserted_ids,
                                                ignore_frequency=10)
    print("Data loading successful.")
    print_db_size()
