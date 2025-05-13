import json
from SPARQLWrapper import SPARQLWrapper

if __name__ == "__main__":

    config = json.load(open("config.json", "r"))

    url_server = config["URL_endpoint"]
    sparql = SPARQLWrapper(url_server)
    sparql.setReturnFormat('json')
    sparql.method = 'GET'

    query = """
        PREFIX sd:<http://www.w3.org/ns/sparql-service-description#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
        SELECT (COUNT(DISTINCT ?kg) as ?count) 
        WHERE {
            ?kg rdf:type sd:Graph.
        }
    """

    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        count_kg = int(result["count"]["value"])

    kg_to_add = json.load(open("KGtoAdd.json", "r"))
    kg_uri = "<http://KG_Nexus.com/Graph_{count_kg}>"

    with open("data_dump.nt", "w", encoding="UTF-8") as f_out:
        f_out.write(f"{kg_uri} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/sparql-service-description#Graph>.\n")
        
        for title in kg_to_add["title"]:
            f_out.write(f'{kg_uri} <http://purl.org/dc/terms/title> "{title["value"]}"@{title["lang"]}.\n')
        
        for description in kg_to_add["description"]:
            f_out.write(f'{kg_uri} <http://purl.org/dc/terms/description> "{description["value"]}"@{description["lang"]}.\n')
        
        f_out.write(f'{kg_uri} <https://schema.org/downloadUrl> "{kg_to_add["downloadURI"]}".\n')        
        
        for prop_kg in kg_to_add["propertyUsed"]:
            f_out.write(f'{kg_uri} <http://www.w3.org/ns/prov#used> <{prop_kg}>.\n')
        
        for class_kg in kg_to_add["classUsed"]:
            f_out.write(f'{kg_uri} <http://www.w3.org/ns/prov#used> <{class_kg}>.\n')


    if kg_to_add["checkUsage"] == "True":
        entities_to_check = [prop_kg for prop_kg in kg_to_add["propertyUsed"]]
        entities_to_check += [class_kg for class_kg in kg_to_add["classUsed"]]
        entities_not_existing = set(entities_to_check)
        step = 100 
        for i in range(0, len(entities_to_check), step):
            query = """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                SELECT DISTINCT ?entities_to_check
                WHERE {
                    VALUES ?entities_to_check {
                        <"""+"> <".join(entities_to_check[i:i+step])+""">
                    }
                    ?entities_to_check rdf:type ?types
                    VALUES ?types {
                        rdfs:Class rdf:Property
                    }
                }
            """

            sparql.setQuery(query)
            response = sparql.queryAndConvert()
            for result in response["results"]["bindings"]:
                entity = result["entities_to_check"]["value"]
                entities_not_existing.remove(entity)
            
        if len(entities_not_existing):
            print(f"WARNING : {len(entities_not_existing)} classes or properties do(es) not exist(s).")
            with open("./listOfUnknownComponents.tsv", "w", encoding="UTF-8") as f_out:
                for e in entities_not_existing:
                    f_out.write(f"{e}\n")


    
    

