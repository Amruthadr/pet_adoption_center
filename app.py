from typing import Dict, List
from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["pet_adoption"]


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


def get_top_10_owners():
    owners_collection = db["owner"]
    top_10_owners = list(owners_collection.find().sort("_id", 1).limit(10))
    return top_10_owners


def update_dog_name(dog_id: object, new_name: str):
    collection = db["dog"]
    result = collection.update_one({"_id": ObjectId(dog_id)}, {"$set": {"name": new_name}})
    return result.modified_count


def add_owner(owner: Dict):
    collection = db["owner"]
    result = collection.insert_one(owner)
    return result.inserted_id


def delete_owner(owner_id: object):
    # Delete owner
    owner_collection = db["owner"]
    owner_result = owner_collection.delete_one({"_id": ObjectId(owner_id)})

    # Find adoption entries
    adoption_collection = db["adoption"]
    adoption_entries = adoption_collection.find({"owner_id": ObjectId(owner_id)})

    # Extract dog_ids from adoption entries
    dog_ids_to_delete = [entry["dog_id"] for entry in adoption_entries]

    # Delete dogs from dog collection based on dog_ids
    dogs_collection = db["dog"]
    dog_result = dogs_collection.delete_many({"_id": {"$in": [ObjectId(dog_id) for dog_id in dog_ids_to_delete]}})

    # Delete adoption entries
    adoption_result = adoption_collection.delete_many({"owner_id": ObjectId(owner_id)})

    print(f"Deleted {owner_result.deleted_count} owner(s).")
    print(f"Deleted {dog_result.deleted_count} dog(s) associated with the owner.")
    print(f"Deleted {adoption_result.deleted_count} adoption entry/entries associated with the owner.")


def get_unique_cities() -> List[str]:
    collection = db["owner"]
    pipeline = [
        {"$group": {"_id": "$address.city"}},
        {"$project": {"_id": 0, "city": "$_id"}},
        {"$limit": 10}
    ]
    result = collection.aggregate(pipeline)
    return [doc["city"] for doc in result]


def get_unique_zip_codes() -> List[str]:
    collection = db["owner"]
    pipeline = [
        {"$group": {"_id": "$address.zip"}},
        {"$project": {"_id": 0, "zip": "$_id"}},
        {"$limit": 10}
    ]
    result = collection.aggregate(pipeline)
    return [doc["zip"] for doc in result]


def get_unique_countries() -> List[str]:
    collection = db["owner"]
    pipeline = [
        {"$group": {"_id": "$address.country"}},
        {"$project": {"_id": 0, "country": "$_id"}},
        {"$limit": 10}
    ]
    result = collection.aggregate(pipeline)
    return [doc["country"] for doc in result]


def get_owner_emails() -> List[str]:
    collection = db["owner"]
    owners = collection.find({}, {"email": 1}).limit(5)
    return list(set([owner["email"] for owner in owners]))


def get_top_5_dogs() -> List[Dict]:
    collection = db["dog"]
    dogs = collection.find({}).sort("_id", 1).limit(5)
    return list(dogs)


def search_owners_by_city(city: str) -> List[Dict]:
    collection = db["owner"]
    owners = collection.find({"address.city": city}).limit(10)
    return list(owners)


def search_owners_by_zip(zip_code: str) -> List[Dict]:
    collection = db["owner"]
    owners = collection.find({"address.zip": zip_code}).limit(10)
    return list(owners)


def search_owners_by_country(country: str) -> List[Dict]:
    collection = db["owner"]
    owners = collection.find({"address.country": country}).limit(10)
    return list(owners)


def count_owners_by_city(city: str) -> int:
    collection = db["owner"]
    count = collection.count_documents({"address.city": city})
    return count


def count_owners_by_zip(zip_code: str) -> int:
    collection = db["owner"]
    count = collection.count_documents({"address.zip": zip_code})
    return count


def count_owners_by_country(country: str) -> int:
    collection = db["owner"]
    count = collection.count_documents({"address.country": country})
    return count


def delete_dog_entry(dog_id: str):
    dogs_collection = db["dog"]
    dog_result = dogs_collection.delete_one({"_id": ObjectId(dog_id)})

    adoption_collection = db["adoption"]
    adoption_result = adoption_collection.delete_many({"dog_id": ObjectId(dog_id)})

    print(f"Removed {dog_result.deleted_count} dog listing.")
    print(f"Removed {adoption_result.deleted_count} adoption entries.")


def count_dogs_by_breed(breed_name):
    dogs_collection = db['dog']
    breed_count = dogs_collection.count_documents({"breed.name": breed_name})
    return breed_count


def search_dogs_by_owner_email(email: str) -> List[Dict]:
    owner_collection = db["owner"]
    adoption_collection = db["adoption"]
    dog_collection = db["dog"]

    owner = owner_collection.find_one({"email": email})
    if not owner:
        return None, []

    adopted_dog_ids = adoption_collection.find({"owner_id": owner["_id"]}).limit(10)
    adopted_dog_ids_list = [doc["dog_id"] for doc in adopted_dog_ids]

    dogs = dog_collection.find({"_id": {"$in": adopted_dog_ids_list}})
    return owner, list(dogs)


def find_top_5_unique_breeds():
    dogs_collection = db["dog"]
    top_5_breeds = list(dogs_collection.aggregate([
        {"$group": {"_id": "$breed.name"}},
        {"$limit": 5}
    ]))
    return [breed['_id'] for breed in top_5_breeds]


def list_top_10_owners():
    top_10_owners = get_top_10_owners()
    print("\nTop 10 Owners")
    print("-------------")
    for index, owner in enumerate(top_10_owners):
        print(f"{index + 1}. {owner['name']} (ID: {owner['_id']})")
        print(f"   Address: {owner['address']}")


def display_top_10_dogs():
    dogs_collection = db["dog"]
    top_10_dogs = dogs_collection.find().limit(10)

    print("\nTop 10 dogs")
    print("----------------")
    for dog in top_10_dogs:
        print(f"ID: {dog['_id']}, Name: {dog['name']}, Breed: {dog['breed']['name']}")


def find_top_5_dogs_not_adopted():
    dog_collection = db["dog"]
    adoption_collection = db["adoption"]
    dogs_not_adopted = dog_collection.find(
        {"_id": {"$nin": adoption_collection.distinct("dog_id")}},
        {"_id": 1, "name": 1, "breed.name": 1}
    ).limit(5)
    return list(dogs_not_adopted)


def adopt_new_pet(owner_id: str, dog_id: str):
    collection = db["adoption"]
    adoption_data = {
        "owner_id": ObjectId(owner_id),
        "dog_id": ObjectId(dog_id),
        "adoption_date": datetime.utcnow()
    }
    result = collection.insert_one(adoption_data)
    return result.inserted_id


if __name__ == "__main__":
    while True:

        try:
            print("\nPet Adoption Console Application")
            print("--------------------------------")
            print("Choose an option:")
            print("1. Find top 5 dogs of a specific breed")
            print("2. Find top 5 dogs adopted by a specific owner")
            print("3. Update Dog name by id")
            print("4. Add a new owner")
            print("5. Delete owner")
            print("6. Search owners by city")
            print("7. Search owners by zip")
            print("8. Search owners by country")
            print("9. Search for the number of owners in a city")
            print("10. Search for the number of owners in a zip")
            print("11. Search for the number of owners in a country")
            print("12. Delete a dog's listing")
            print("13. Search for the count of specific dog breeds listed for adoption")
            print("14. Adopt a new pet")
            print("15. Search for pets by owner's email address")
            print("16. Display top 10 owners")

            print("Any other. Exit")

            choice = int(input("Enter your choice (1-16): "))

            if choice == 1:
                top_5_breeds = find_top_5_unique_breeds()
                print(f"Top 5 unique dog breeds: {', '.join(top_5_breeds)}")
                breed_name = input("Enter the dog breed name: ")
                top_5_dogs = find_top_5_dogs_of_breed(breed_name)
                print("\nTop 5 dogs of breed", breed_name)
                print("----------------------------")
                for index, dog in enumerate(top_5_dogs):
                    print(f"{index + 1}. {dog['name']} (ID: {dog['_id']})")
            elif choice == 2:
                list_top_10_owners()
                owner_id = input("Enter the owner ID: ")
                top_5_dogs = find_top_5_dogs_by_owner(owner_id)
                print(f"\nTop 5 dogs adopted by owner ID {owner_id}")
                print("------------------------------------")
                for index, dog in enumerate(top_5_dogs):
                    print(f"{index + 1}. {dog['name']} (ID: {dog['_id']})")
            elif choice == 3:
                display_top_10_dogs()
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
                list_top_10_owners()
                owner_id = input("Enter the owner ID: ")
                delete_owner(owner_id)
            elif choice == 6:
                cities = get_unique_cities()
                for index, city in enumerate(cities):
                    print(f"{index + 1}. {city}")
                city = input("Enter the city to search for owners: ")
                owners = search_owners_by_city(city)
                if not owners:
                    print(f"No owners found in {city}.")
                else:
                    print(f"\nOwners in {city}:")
                    print("-----------------")
                    for index, owner in enumerate(owners):
                        print(f"{index + 1}. {owner['name']} (ID: {owner['_id']})")

            elif choice == 7:
                zips = get_unique_zip_codes()
                for index, zip_code in enumerate(zips):
                    print(f"{index + 1}. {zip_code}")
                zip_code = input("Enter the zip code to search for owners: ")
                owners = search_owners_by_zip(zip_code)
                if not owners:
                    print(f"No owners found in zip code {zip_code}.")
                else:
                    print(f"\nOwners in zip code {zip_code}:")
                    print("--------------------------")
                    for index, owner in enumerate(owners):
                        print(f"{index + 1}. {owner['name']} (ID: {owner['_id']})")

            elif choice == 8:
                countries = get_unique_countries()
                for index, country in enumerate(countries):
                    print(f"{index + 1}. {country}")
                country = input("Enter the country to search for owners: ")
                owners = search_owners_by_country(country)
                if not owners:
                    print(f"No owners found in {country}.")
                else:
                    print(f"\nOwners in {country}:")
                    print("-------------------")
                    for index, owner in enumerate(owners):
                        print(f"{index + 1}. {owner['name']} (ID: {owner['_id']})")

            elif choice == 9:
                cities = get_unique_cities()
                for index, city in enumerate(cities):
                    print(f"{index + 1}. {city}")
                city = input("Enter the city to count owners: ")
                count = count_owners_by_city(city)
                print(f"There are {count} owners in {city}.")

            elif choice == 10:
                zips = get_unique_zip_codes()
                for index, zip_code in enumerate(zips):
                    print(f"{index + 1}. {zip_code}")
                zip_code = input("Enter the zip code to count owners: ")
                count = count_owners_by_zip(zip_code)
                print(f"There are {count} owners in zip code {zip_code}.")

            elif choice == 11:
                countries = get_unique_countries()
                for index, country in enumerate(countries):
                    print(f"{index + 1}. {country}")
                country = input("Enter the country to count owners: ")
                count = count_owners_by_country(country)
                print(f"There are {count} owners in {country}.")

            elif choice == 12:
                dogs = get_top_5_dogs()
                if not dogs:
                    print("No dogs found.")
                else:
                    print("\nTop 5 dogs:")
                    print("-----------")
                    for index, dog in enumerate(dogs):
                        print(f"{index + 1}. {dog['name']} (ID: {dog['_id']})")

                    dog_id = input("Enter the dog ID to remove: ")
                    try:
                        delete_dog_entry(ObjectId(dog_id))
                    except Exception as e:
                        print("Error:", e)
            elif choice == 13:
                breeds = find_top_5_unique_breeds()
                for index, breed in enumerate(breeds):
                    print(f"{index + 1}. {breed}")
                breed_name = input("Enter the dog breed to count: ")
                count = count_dogs_by_breed(breed_name)
                print(f"There are {count} {breed_name} dogs listed for adoption.")
            # print("14. Adopt a new pet")
            # print("15. Search for pets by owner's email address")
            elif choice == 14:
                print("\nTop 10 owners list:")
                list_top_10_owners()
                print("\nTop 5 dogs not adopted:")
                dogs = find_top_5_dogs_not_adopted()
                for index, dog in enumerate(dogs):
                    print(f"{index + 1}. ID: {dog['_id']} Name: {dog['name']} Breed: {dog['breed']['name']}")

                owner_id = input("\nEnter the owner ID: ")
                dog_id = input("Enter the dog ID: ")

                adoption_id = adopt_new_pet(owner_id, dog_id)
                print(f"Adoption entry created with ID: {adoption_id}")

            elif choice == 15:
                top_5_emails = get_owner_emails()
                print("Top 5 owner emails:\n" + '\n'.join(top_5_emails))
                email = input("\nEnter the owner's email address: ")
                owner, adopted_dogs = search_dogs_by_owner_email(email)

                if owner:
                    print(f"\nOwner: {owner['name']} (ID: {owner['_id']}) Email: {owner['email']}")
                    if adopted_dogs:
                        print("\nAdopted pets:")
                        for index, dog in enumerate(adopted_dogs):
                            print(f"{index + 1}. Name: {dog['name']} (ID: {dog['_id']}) Breed: {dog['breed']['name']}")
                    else:
                        print("\nNo pets are adopted by this owner yet.")
                else:
                    print("No owner found with the given email address.")

            elif choice == 16:
                list_top_10_owners()
            else:
                print("Invalid choice. Please try again.")
        except Exception as e:
            print(e)
