import json
from rdflib import Graph
import requests
import configparser
import os.path

INPUT_FILE = ""
AUTH = {}
VERBOSE = False

config = configparser.ConfigParser()
if not os.path.isfile("config.ini"):
    config["Authentication"] = {
        'key_identity': '',
        'key_credential': ''
    }
    config['RDF'] = {
        'input_file': ''
    }
    config['Debug'] = {
        'verbose': 'yes'
    }

    with open('config.ini', 'w') as configfile:
        config.write(configfile)

    print("Created config.ini file. Fill it to start the import.")
    exit(0)
else:
    config.read("config.ini")
    AUTH = config['Authentication']
    INPUT_FILE = config['RDF']['input_file']
    VERBOSE = config.getboolean('Debug', 'verbose')

    if AUTH['key_identity'] == '' or AUTH['key_credential'] == '':
        print("Authentication variables must be set")
        exit(1)
    elif not os.path.isfile(INPUT_FILE):
        print("input_file does not exist.")
        exit(1)

g = Graph()
g.bind("crm", "http://www.cidoc-crm.org/cidoc-crm/")
g.parse(INPUT_FILE, format="xml")

classes = {}
grouped_triples = {}

def map_classes():
    init_size = requests.get("http://70.34.242.54/api/resource_classes?per_page=10").headers["omeka-s-total-results"]
    classes_json = requests.get(f"http://70.34.242.54/api/resource_classes?per_page={init_size}").json()
    for _class in classes_json:
        classes[_class['o:term']] = _class['o:id']

def group_triples():
    for subject, predicate, obj in g:
        if subject not in grouped_triples:
            grouped_triples[subject] = {
                "id": 0,
                "predicates": {}
            }

        if predicate not in grouped_triples[subject]["predicates"]:
            grouped_triples[subject]["predicates"][predicate] = []

        grouped_triples[subject]["predicates"][predicate].append(obj)

    # #####################
    # #    DESCRIPTION    #
    # #####################
    # This function will import RDF data into Omeka S. The process is done in two parts:
    # 1. First pass - script will send HTTP requests to Omeka with empty objects to generate their internal
    #    Omeka Resource ID and assign it to our internal dictionary.
    # 2. Second pass - using the internal Omeka Resource ID, the script will send a PATCH request adding the missing
    #     information. Using the previously generated IDs, we can create connections between objects: If Object A
    #    references Object B with a URI, we can do a lookup on Object B's URI so we can assign its Omeka Resource ID as
    #    a parameter. If the object URI is not in the dictionary, we can safely just add it as URI property type.
def create_omeka_items():
    # First pass
    for subject, data in grouped_triples.items():
        # Skip CIDOC duplicates from ontology
        if subject.startswith("http://www.cidoc-crm.org/"):
            continue
        json_str = {
            "o:resource_class": {
                "o:id": get_omeka_class_id(data["predicates"]),
            }
        }
        res = requests.post("http://70.34.242.54/api/items", json=json_str, params=AUTH).json()
        data["id"] = res['o:id']

    # Second Pass
    for subject, data in grouped_triples.items():
        # Skip CIDOC duplicates from ontology
        if subject.startswith("http://www.cidoc-crm.org/"):
            continue
        if VERBOSE:
            print(data['id'])
        json_str = {}

        for predicate, objects in data["predicates"].items():
            if predicate.n3(g.namespace_manager) == "rdf:type":
                continue

            json_str[predicate.n3(g.namespace_manager)] = []
            if (predicate.n3(g.namespace_manager) == "rdfs:label"):
                json_str["dcterms:title"] = []
            for obj in objects:
                # If URI
                create_property(json_str, obj, predicate.n3(g.namespace_manager))
                if predicate.n3(g.namespace_manager) == "rdfs:label":
                    create_property(json_str, obj, "dcterms:title")
        requests.patch(f"http://70.34.242.54/api/items/{data['id']}", json=json_str, params=AUTH)
        debug_print_info(json_str, data)


def create_property(json_str, obj, predicate):
    if obj.n3(g.namespace_manager).startswith('<') and obj.n3(g.namespace_manager).endswith('>'):
        # Object is in our dictionary, therefore should be in Omeka
        if obj in grouped_triples:
            subject_id = grouped_triples[obj]['id']
            json_str[predicate].append({
                "type": "resource",
                "property_id": "auto",
                "value_resource_id": subject_id
            })
        else:
            json_str[predicate].append({
                "type": "uri",
                "property_id": "auto",
                "@id": obj
            })

    else:
        json_str[predicate].append({
            "type": "literal",
            "property_id": "auto",
            "@value": obj
        })


def get_omeka_class_id(predicates):
    for predicate, objects in predicates.items():
        for obj in objects:
            if predicate.n3(g.namespace_manager) == "rdf:type":
                return classes.get(obj.n3(g.namespace_manager))

def debug_print_triples():
    if not VERBOSE:
        return

    for subject, data in grouped_triples.items():
        print(f"{subject.n3(g.namespace_manager)}")
        for predicate, objects in data["predicates"].items():
            print(f"  {predicate.n3(g.namespace_manager)}")
            for obj in objects:
                if predicate.n3(g.namespace_manager) == "rdf:type":
                    print(f"    {classes.get(obj.n3(g.namespace_manager))}")
                else:
                    print(f"    {obj.n3(g.namespace_manager)}")

def debug_print_info(json_str, data):
    if not VERBOSE:
        return

    print(json.dumps(json_str, indent=4))
    for predicate, objects in data["predicates"].items():
        print(f"  {predicate.n3(g.namespace_manager)}")
        for obj in objects:
            if predicate.n3(g.namespace_manager) == "rdf:type":
                print(f"    {classes.get(obj.n3(g.namespace_manager))}")
            else:
                print(f"    {obj.n3(g.namespace_manager)}")

map_classes()
group_triples()
# debug_print_triples()
create_omeka_items()