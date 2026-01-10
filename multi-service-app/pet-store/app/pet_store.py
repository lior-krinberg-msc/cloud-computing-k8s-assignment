import os
import re
import uuid
from datetime import datetime

import requests
from flask import Flask, jsonify, request, send_file
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB connection
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
DB_NAME = os.environ.get('DB_NAME', 'pet_store')  # DB_NAME is from docker compose so each store will get a separate db

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
pet_types_collection = db['pet_types']
pets_collection = db['pets']

# Counter collection for auto-increment IDs
counters_collection = db['counters']

NINJA_API_KEY = os.environ.get('NINJA_API_KEY')
NINJA_URL = 'https://api.api-ninjas.com/v1/animals'


def get_next_pet_type_id():
    result = counters_collection.find_one_and_update(
        {'_id': 'pet_type_id'},
        {'$inc': {'seq': 1}},
        upsert=True,
        return_document=True
    )
    return str(result['seq'])


# -- Helper functions --
def parse_lifespan(text):
    if not text or not isinstance(text, str):
        return None
    numbers = re.findall(r'\d+', text)
    if not numbers:
        return None
    return min(int(n) for n in numbers)

def parse_date(date_str):
    try:
        return datetime.strptime(date_str.strip(), '%d-%m-%Y')
    except Exception:
        return None

def extract_words(text):
    if not text:
        return []
    return re.findall(r'\b\w+\b', text)

def download_image(url):
    try:
        resp = requests.get(url)
        if resp.status_code != 200:
            return None

        url_path = url.split('?')[0]  # remove query parameters
        ext = os.path.splitext(url_path)[1].lower() or '.jpg'

        if ext == '.jpeg':
            ext = '.jpg'
        fname = f"{uuid.uuid4()}{ext}"

        os.makedirs('images', exist_ok=True)
        path = os.path.join('images', fname)

        with open(path, 'wb') as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        return fname

    except Exception:
        return None

def clean_pet(p):
    return {
        "name": p.get("name"),
        "birthdate": p.get("birthdate", "NA"),
        "picture": p.get("picture", "NA")
    }

def clean_pet_type(ptype):
    return {
        "id": ptype.get("id"),
        "type": ptype.get("type"),
        "family": ptype.get("family"),
        "genus": ptype.get("genus"),
        "attributes": ptype.get("attributes", []),
        "lifespan": ptype.get("lifespan"),
        "pets": ptype.get("pets", [])
    }


def remove_image_file(filename):
    if not filename or filename == "NA":
        return
    path = os.path.join("images", filename)
    if os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass

# -- pet-types endpoints --

@app.route('/pet-types', methods=['GET'])
def get_pet_types():
    params = request.args.to_dict()

    all_types = list(pet_types_collection.find())

    if not params:
        return jsonify([clean_pet_type(p) for p in all_types]), 200

    out = []
    for ptype in all_types:
        ok = True
        for key, val in params.items():
            if key == "hasAttribute":
                attrs = ptype.get("attributes", [])
                if val.lower() not in (a.lower() for a in attrs):
                    ok = False
                    break
                continue

            if key in ["id", "type", "family", "genus", "lifespan"]:
                field_val = ptype.get(key)
                if field_val is None:
                    ok = False
                    break
                if key == "lifespan":
                    if str(field_val) != val:
                        ok = False
                        break
                else:
                    if str(field_val).lower() != val.lower():
                        ok = False
                        break
        if ok:
            out.append(clean_pet_type(ptype))
    return jsonify(out), 200

@app.route('/pet-types', methods=['POST'])
def add_pet_type():
    try:
        if request.headers.get("Content-Type") != "application/json":
            return jsonify({"error": "Expected application/json media type"}), 415

        data = request.get_json()
        if not data or 'type' not in data or len(data) != 1:
            return jsonify({"error": "Malformed data"}), 400

        requested_type = data['type']

        # Check if type already exists
        existing = pet_types_collection.find_one({
            'type': {'$regex': f'^{re.escape(requested_type)}$', '$options': 'i'}
        })
        if existing:
            return jsonify({"error": "Pet type already exists"}), 400

        headers = {"X-Api-Key": NINJA_API_KEY}
        params = {"name": requested_type}

        try:
            resp = requests.get(NINJA_URL, headers=headers, params=params, timeout=10)
        except requests.exceptions.SSLError:
            # Retry with SSL verification disabled if SSL fails (for corporate proxies/firewalls)
            resp = requests.get(NINJA_URL, headers=headers, params=params, verify=False, timeout=10)

        if resp.status_code != 200:
            return jsonify({"server error": f"API response code {resp.status_code}"}), 500

        results = resp.json()
        if not results:
            return jsonify({"error": "Pet type not found"}), 400

        chosen = None
        for item in results:
            if item.get("name", "").lower() == requested_type.lower():
                chosen = item
                break

        if chosen is None:
            return jsonify({"error": "Pet type not found"}), 400

        new_id = get_next_pet_type_id()

        chars = chosen.get("characteristics") or {}
        lifespan = parse_lifespan(chars.get("lifespan")) if "lifespan" in chars else None

        if chars.get("temperament"):
            attrs = extract_words(chars["temperament"])
        elif chars.get("group_behavior"):
            attrs = extract_words(chars["group_behavior"])
        else:
            attrs = []

        taxonomy = chosen.get("taxonomy", {})
        family = taxonomy.get("family", "")
        genus = taxonomy.get("genus", "")
        ptype = chosen.get("name", requested_type)

        new_ptype = {
            "id": new_id,
            "type": ptype,
            "family": family,
            "genus": genus,
            "attributes": attrs,
            "lifespan": lifespan,
            "pets": []
        }

        pet_types_collection.insert_one(new_ptype)
        return jsonify(clean_pet_type(new_ptype)), 201

    except Exception as e:
        print("Error in add_pet_type:", e)
        return jsonify({"error": "Server error"}), 500


@app.route('/pet-types/<pet_type_id>', methods=['GET'])
def get_pet_type_by_id(pet_type_id):
    ptype = pet_types_collection.find_one({"id": pet_type_id})
    if not ptype:
        return jsonify({"error": "Not found"}), 404
    return jsonify(clean_pet_type(ptype)), 200


@app.route('/pet-types/<pet_type_id>', methods=['DELETE'])
def delete_pet_type(pet_type_id):
    ptype = pet_types_collection.find_one({"id": pet_type_id})
    if not ptype:
        return jsonify({"error": "Not found"}), 404

    if ptype.get("pets"):
        return jsonify({"error": "Malformed data"}), 400

    pet_types_collection.delete_one({"id": pet_type_id})
    return "", 204


@app.route('/pet-types/<pet_type_id>', methods=['PUT'])
def put_not_allowed(pet_type_id):
    return jsonify({"error": "Can not update pet type"}), 405

# -- pets endpoints --
@app.route('/pet-types/<pet_type_id>/pets', methods=['POST'])
def add_pet(pet_type_id):
    try:
        ptype = pet_types_collection.find_one({"id": pet_type_id})
        if not ptype:
            return jsonify({"error": "Not found"}), 404

        if request.headers.get("Content-Type") != "application/json":
            return jsonify({"error": "Expected application/json media type"}), 415

        data = request.get_json()
        if not data or "name" not in data:
            return jsonify({"error": "Malformed data"}), 400

        name = data["name"]
        birthdate = data.get("birthdate") or "NA"
        pic_url = data.get("picture-url", None)

        if any(n.lower() == name.lower() for n in ptype.get("pets", [])):
            return jsonify({"error": "Malformed data"}), 400

        picture = "NA"
        if pic_url:
            f = download_image(pic_url)
            if f:
                picture = f

        pet_obj = {
            "pet_type_id": pet_type_id,
            "name": name,
            "name_lower": name.lower(),
            "birthdate": birthdate,
            "picture": picture,
            "_picture_url": pic_url
        }

        pets_collection.insert_one(pet_obj)

        # Update pet_types with the pet name
        pet_types_collection.update_one(
            {"id": pet_type_id},
            {"$push": {"pets": name}}
        )

        return jsonify(clean_pet(pet_obj)), 201

    except Exception as e:
        print("Error in add_pet:", e)
        return jsonify({"error": "Server error"}), 500


@app.route('/pet-types/<pet_type_id>/pets', methods=['GET'])
def get_pets(pet_type_id):
    ptype = pet_types_collection.find_one({"id": pet_type_id})
    if not ptype:
        return jsonify({"error": "Not found"}), 404

    all_pets = list(pets_collection.find({"pet_type_id": pet_type_id}))
    all_pets_clean = [clean_pet(p) for p in all_pets]

    params = request.args.to_dict()
    if not params:
        return jsonify(all_pets_clean), 200

    gt = params.get("birthdateGT")
    lt = params.get("birthdateLT")

    date_gt = parse_date(gt) if gt else None
    if gt and date_gt is None:
        return jsonify({"error": "Invalid date format for birthdateGT"}), 400

    date_lt = parse_date(lt) if lt else None
    if lt and date_lt is None:
        return jsonify({"error": "Invalid date format for birthdateLT"}), 400

    out = []
    for p in all_pets_clean:
        bd = p.get("birthdate")
        if bd == "NA":
            continue

        actual = parse_date(bd)
        if not actual:
            continue

        ok = True
        if date_gt and actual <= date_gt:
            ok = False
        if date_lt and actual >= date_lt:
            ok = False

        if ok:
            out.append(p)

    return jsonify(out), 200


# -- individual pet endpoints --

@app.route('/pet-types/<pet_type_id>/pets/<name>', methods=['GET'])
def get_pet_by_name(pet_type_id, name):
    ptype = pet_types_collection.find_one({"id": pet_type_id})
    if not ptype:
        return jsonify({"error": "Not found"}), 404

    pet = pets_collection.find_one({
        "pet_type_id": pet_type_id,
        "name_lower": name.lower()
    })
    if not pet:
        return jsonify({"error": "Not found"}), 404

    return jsonify(clean_pet(pet)), 200


@app.route('/pet-types/<pet_type_id>/pets/<name>', methods=['PUT'])
def update_pet(pet_type_id, name):
    try:
        ptype = pet_types_collection.find_one({"id": pet_type_id})
        if not ptype:
            return jsonify({"error": "Not found"}), 404

        if request.headers.get("Content-Type") != "application/json":
            return jsonify({"error": "Expected application/json media type"}), 415

        data = request.get_json()
        if not data or "name" not in data:
            return jsonify({"error": "Malformed data"}), 400

        pet = pets_collection.find_one({
            "pet_type_id": pet_type_id,
            "name_lower": name.lower()
        })
        if not pet:
            return jsonify({"error": "Not found"}), 404

        new_name = data["name"]
        new_birthdate = data.get("birthdate")
        new_url = data.get("picture-url")

        cur_url = pet.get("_picture_url")
        cur_pic = pet.get("picture", "NA")

        if new_url is None:
            # picture-url not supplied, reset to NA
            if cur_pic != "NA":
                remove_image_file(cur_pic)
            cur_pic = "NA"
            new_picture_url = None
        elif new_url == cur_url:
            new_picture_url = cur_url
        else:
            if cur_pic != "NA":
                remove_image_file(cur_pic)
            f = download_image(new_url)
            if f:
                cur_pic = f
            else:
                cur_pic = "NA"
            new_picture_url = new_url

        update_fields = {
            "name": new_name,
            "name_lower": new_name.lower(),
            "birthdate": new_birthdate if new_birthdate is not None else "NA",
            "picture": cur_pic,
            "_picture_url": new_picture_url
        }

        pets_collection.update_one(
            {"pet_type_id": pet_type_id, "name_lower": name.lower()},
            {"$set": update_fields}
        )

        # Update pet names in pet_types if name changed
        if new_name.lower() != name.lower():
            pet_types_collection.update_one(
                {"id": pet_type_id},
                {"$pull": {"pets": {"$regex": f"^{re.escape(name)}$", "$options": "i"}}}
            )
            pet_types_collection.update_one(
                {"id": pet_type_id},
                {"$push": {"pets": new_name}}
            )

        updated_pet = pets_collection.find_one({
            "pet_type_id": pet_type_id,
            "name_lower": new_name.lower()
        })

        return jsonify(clean_pet(updated_pet)), 200

    except Exception as e:
        print("Error in update_pet:", e)
        return jsonify({"error": "Server error"}), 500


@app.route('/pet-types/<pet_type_id>/pets/<name>', methods=['DELETE'])
def delete_pet(pet_type_id, name):
    ptype = pet_types_collection.find_one({"id": pet_type_id})
    if not ptype:
        return jsonify({"error": "Not found"}), 404

    pet = pets_collection.find_one({
        "pet_type_id": pet_type_id,
        "name_lower": name.lower()
    })
    if not pet:
        return jsonify({"error": "Not found"}), 404

    remove_image_file(pet.get("picture", "NA"))

    # Remove from pets collection
    pets_collection.delete_one({
        "pet_type_id": pet_type_id,
        "name_lower": name.lower()
    })

    # Remove pet name from pet_types
    pet_types_collection.update_one(
        {"id": pet_type_id},
        {"$pull": {"pets": pet["name"]}}
    )

    return "", 204


# -- pictures endpoint --

@app.route('/pictures/<path:file_name>', methods=['GET'])
def get_picture(file_name):
    path = os.path.join("images", file_name)
    if not os.path.exists(path):
        return jsonify({"error": "Not found"}), 404

    ext = os.path.splitext(file_name)[1].lower()
    if ext == '.png':
        mtype = 'image/png'
    elif ext in ['.jpg', '.jpeg']:
        mtype = 'image/jpeg'
    else:
        mtype = 'image/jpeg'

    return send_file(path, mimetype=mtype), 200

@app.route('/kill', methods=['GET'])
def kill_container():
    os._exit(1)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    print(f"Running pets server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=True)
