from typing import Dict, List

from bson import ObjectId
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["pet_adoption"]  # Replace "your_database_name" with the desired name of your MongoDB database


def find_top_5_dogs_of_breed(breed_name: str) -> List[Dict]:
    collection = db["dog"]
    dogs = collection.find({"breed.name": breed_name}).sort("name", 1).limit(5)
    return list(dogs)


def find_top_5_dogs_by_owner(owner_id: str) -> List[Dict]:
    adoption_collection = db["adoption"]
    dog_collection = db["dog"]

    adopted_dog_ids = adoption_collection.find({"owner_id": ObjectId(owner_id)}).limit(5)
    adopted_dog_ids_list = [doc["dog_id"] for doc in adopted_dog_ids]

    dogs = dog_collection.find({"_id": {"$in": adopted_dog_ids_list}}).sort("name", 1)
    return list(dogs)


def update_dog_name(dog_id: object, new_name: str):
    collection = db["dog"]
    result = collection.update_one({"_id": dog_id}, {"$set": {"name": new_name}})
    return result.modified_count


def add_owner(owner: Dict):
    collection = db["owner"]
    result = collection.insert_one(owner)
    return result.inserted_id


def delete_owner(owner_id: object):
    collection = db["owner"]
    result = collection.delete_one({"_id": owner_id})
    return result.deleted_count


if __name__ == "__main__":
    while True:
        print("\nPet Adoption Console Application")
        print("--------------------------------")
        print("Choose an option:")
        print("1. Find top 5 dogs of a specific breed")
        print("2. Find top 5 dogs adopted by a specific owner")
        print("3. Update Dog name by id")
        print("4. Add a new owner")
        print("5. Delete owner")
        print("Any other key. Exit")

        choice = int(input("Enter your choice (1-5): "))

        if choice == 1:
            breed_name = input("Enter the dog breed name: ")
            top_5_dogs = find_top_5_dogs_of_breed(breed_name)
            print("\nTop 5 dogs of breed", breed_name)
            print("----------------------------")
            for index, dog in enumerate(top_5_dogs):
                print(f"{index + 1}. {dog['name']} (ID: {dog['_id']})")
        elif choice == 2:
            owner_id = input("Enter the owner ID: ")
            top_5_dogs = find_top_5_dogs_by_owner(owner_id)
            print(f"\nTop 5 dogs adopted by owner ID {owner_id}")
            print("------------------------------------")
            for index, dog in enumerate(top_5_dogs):
                print(f"{index + 1}. {dog['name']} (ID: {dog['_id']})")
        elif choice == 3:
            dog_id = input("Enter the dog ID: ")
            new_name = input("Enter the new name: ")
            modified_count = update_dog_name(dog_id, new_name)
            print(f"Updated {modified_count} dog's name.")
        elif choice == 4:
            name = input("Enter the owner's name: ")
            email = input("Enter the owner's email: ")
            mobile = input("Enter the owner's mobile number: ")
            street = input("Enter the owner's street address: ")
            city = input("Enter the owner's city: ")
            country = input("Enter the owner's country: ")
            zip_code = input("Enter the owner's zip code: ")

            owner = {
                "name": name,
                "email": email,
                "mobile": mobile,
                "address": {
                    "street": street,
                    "city": city,
                    "country": country,
                    "zip": zip_code
                }
            }
            owner_id = add_owner(owner)
            print(f"Added new owner with ID: {owner_id}")
        elif choice == 5:
            owner_id = input("Enter the owner ID: ")
            deleted_count = delete_owner(owner_id)
            print(f"Deleted {deleted_count} owner.")
        else:
            print("Invalid choice. Please try again.")
