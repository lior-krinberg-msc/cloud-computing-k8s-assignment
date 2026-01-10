import os
import random
import requests
from flask import Flask, jsonify, request
from pymongo import MongoClient

app = Flask(__name__)

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['pet_orders']
transactions_collection = db['transactions']
counters_collection = db['counters']
PET_STORE1_URL = os.environ.get('PET_STORE1_URL', 'http://pet-store1:5001')
PET_STORE2_URL = os.environ.get('PET_STORE2_URL', 'http://pet-store2:5001')
OWNER_PASSWORD = "LovesPetsL2M3n4"


def get_next_purchase_id():
    result = counters_collection.find_one_and_update(
        {'_id': 'purchase_id'},
        {'$inc': {'seq': 1}},
        upsert=True,
        return_document=True
    )
    return str(result['seq'])


def get_store_url(store_num):
    # return URL for tha matching store number
    if store_num == 1:
        return PET_STORE1_URL
    elif store_num == 2:
        return PET_STORE2_URL
    return None

def find_pet_type_id(store_url, pet_type_name):
    try:
        resp = requests.get(f"{store_url}/pet-types")
        if resp.status_code != 200:
            return None

        pet_types = resp.json()
        for pt in pet_types:
            if pt.get('type', '').lower() == pet_type_name.lower():
                return pt.get('id')
        return None
    except Exception:
        return None


def get_pets_of_type(store_url, pet_type_id):
    try:
        resp = requests.get(f"{store_url}/pet-types/{pet_type_id}/pets")
        if resp.status_code != 200:
            return []
        return resp.json()
    except Exception:
        return []


def delete_pet(store_url, pet_type_id, pet_name):
    try:
        resp = requests.delete(f"{store_url}/pet-types/{pet_type_id}/pets/{pet_name}")
        return resp.status_code == 204
    except Exception:
        return False


def find_available_pet(pet_type_name, store=None, pet_name=None):
    # finda an available pet based on the criteria

    stores_to_check = []

    if store is not None:
        # Check specific store
        stores_to_check = [(store, get_store_url(store))]
    else:
        # Check both stores
        stores_to_check = [(1, PET_STORE1_URL), (2, PET_STORE2_URL)]

    available_pets = []  # List of (store_num, pet_type_id, pet_name)

    for store_num, store_url in stores_to_check:
        # Find pet-type ID in this store
        pet_type_id = find_pet_type_id(store_url, pet_type_name)
        if not pet_type_id:
            continue

        # Get pets of this type
        pets = get_pets_of_type(store_url, pet_type_id)
        if not pets:
            continue

        if pet_name is not None:
            # Looking for specific pet
            for pet in pets:
                if pet.get('name', '').lower() == pet_name.lower():
                    return (store_num, pet_type_id, pet.get('name'))
        else:
            # all other available pets
            for pet in pets:
                available_pets.append((store_num, pet_type_id, pet.get('name')))

    # looking for specific pet and not found
    if pet_name is not None:
        return None

    # return random pet from available ones
    if available_pets:
        return random.choice(available_pets)

    return None


# -- purchases endpoint --
@app.route('/purchases', methods=['POST'])
def create_purchase():
    try:
        # check content type
        if request.headers.get('Content-Type') != 'application/json':
            return jsonify({"error": "Expected application/json media type"}), 415

        data = request.get_json()
        if not data:
            return jsonify({"error": "Malformed data"}), 400

        # validate required fields
        if 'purchaser' not in data or 'pet-type' not in data:
            return jsonify({"error": "Malformed data"}), 400

        # reject extra fields
        allowed_fields = {'purchaser', 'pet-type', 'store', 'pet-name'}
        if not set(data.keys()).issubset(allowed_fields):
            return jsonify({"error": "Malformed data"}), 400

        purchaser = data['purchaser']
        pet_type = data['pet-type']
        store = data.get('store')
        pet_name = data.get('pet-name')

        # check store value (if we got it from the request)
        if store is not None and store not in [1, 2]:
            return jsonify({"error": "Malformed data"}), 400

        # pet-name can only be provided if store is provided
        if pet_name is not None and store is None:
            return jsonify({"error": "Malformed data"}), 400

        # 1. get an available pet
        result = find_available_pet(pet_type, store, pet_name)

        if result is None:
            return jsonify({"error": "No pet of this type is available"}), 400

        chosen_store, pet_type_id, chosen_pet_name = result

        # 2. delete the pet from the store
        store_url = get_store_url(chosen_store)
        if not delete_pet(store_url, pet_type_id, chosen_pet_name):
            return jsonify({"error": "No pet of this type is available"}), 400

        # 3. generate purchase and save in db
        purchase_id = get_next_purchase_id()
        purchase = {
            "purchaser": purchaser,
            "pet-type": pet_type,
            "store": chosen_store,
            "pet-name": chosen_pet_name,
            "purchase-id": purchase_id
        }
        transaction = {
            "purchaser": purchaser,
            "pet-type": pet_type,
            "store": chosen_store,
            "purchase-id": purchase_id
        }
        transactions_collection.insert_one(transaction)

        # purchase completed successfully
        return jsonify(purchase), 201

    except Exception as e:
        print("Error in create_purchase:", e)
        return jsonify({"error": "Server error"}), 500


# -- transactions endpoint --
@app.route('/transactions', methods=['GET'])
def get_transactions():
    # only owner can see transactions
    owner_pc = request.headers.get('OwnerPC')
    if owner_pc != OWNER_PASSWORD:
        return jsonify({"error": "unauthorized"}), 401

    # get query parameters
    params = request.args.to_dict()

    # build db query
    query = {}
    for key, val in params.items():
        if key == 'store':
            query['store'] = int(val)
        elif key == 'purchaser':
            query['purchaser'] = val
        elif key == 'pet-type':
            query['pet-type'] = {'$regex': f'^{val}$', '$options': 'i'}
        elif key == 'purchase-id':
            query['purchase-id'] = val

    # get transactions from db
    transactions = list(transactions_collection.find(query))

    # return the transactions with re relevant fields only
    result = []
    for t in transactions:
        result.append({
            "purchaser": t.get("purchaser"),
            "pet-type": t.get("pet-type"),
            "store": t.get("store"),
            "purchase-id": t.get("purchase-id")
        })

    return jsonify(result), 200

@app.route('/kill', methods=['GET'])
def kill_container():
    os._exit(1)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5003))
    print(f"Running pet-order service on port {port}")
    app.run(host='0.0.0.0', port=port, debug=True)
