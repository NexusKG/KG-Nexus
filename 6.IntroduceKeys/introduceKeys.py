import json
from SPARQLWrapper import SPARQLWrapper

def loadKeys(file):
    keys = []
    props_iri = set()
    line = file.readline()
    while line !="":
        props = line[:-1].split(",")
        props_iri.update({prop for prop in props})
        keys.append(props)
        line = file.readline()
    return keys, list(props_iri)

if __name__ == "__main__":

    config = json.load(open("config.json", "r"))

    url_server = config["URL_endpoint"]
    sparql = SPARQLWrapper(url_server)
    sparql.setReturnFormat('json')
    sparql.method = 'GET'
    
    keys_information = json.load(open("keysInformation.json", "r"))

    query = """
        PREFIX prov:<http://www.w3.org/ns/prov#>
    
        SELECT ?truth
        WHERE {
            BIND ( EXISTS {
                    <"""+keys_information["knowledgeGraph"]+"""> prov:used <"""+keys_information["classUsed"]+""">.
                } as ?truth )
        }
    """

    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        if result["truth"]["value"] != "true":
            print("The Knowledge Graph, the class or the connection between the two does not exists")
            # exit(-1)

    with open(keys_information["dataFile"], "r", encoding="UTF-8") as f_read:
        keys, props_iri = loadKeys(f_read)
    
    with open("./data_dump.nt", "w", encoding="UTF-8") as f_out:
        query = """
            PREFIX prov:<http://www.w3.org/ns/prov#>
        
            SELECT (COUNT(DISTINCT ?key) as ?count)
            WHERE {
                ?key rdf:type <http://KG_Nexus.com/Key>.
            }
        """
        sparql.setQuery(query)
        response = sparql.queryAndConvert()
        for result in response["results"]["bindings"]:
            id_key = int(result["count"]["value"])

        id_next_key = id_key
        for properties in keys:
            f_out.write(f"<http://KG_Nexus.com/Key_{id_next_key}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#> <http://KG_Nexus.com/Key>.\n")
            f_out.write(f"<http://KG_Nexus.com/Key_{id_next_key}> <http://www.w3.org/ns/prov#wasDerivedFrom> <{keys_information['knowledgeGraph']}>.\n")
            f_out.write(f"<http://KG_Nexus.com/Key_{id_next_key}> <http://www.w3.org/ns/prov#used> <{keys_information['classUsed']}>.\n")
            
            for property in properties:
                f_out.write(f"<http://KG_Nexus.com/Key_{id_next_key}> <http://www.w3.org/ns/prov#used> <{property}>.\n")
        
            id_next_key += 1
    
    step = 100
    props_not_linked = set(props_iri)
    for i in range(0, len(props_iri), step):
        query = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?entities_to_check
            WHERE {
                VALUES ?entities_to_check {
                    <"""+"> <".join(props_iri[i:i+step])+""">
                }
                <"""+keys_information["knowledgeGraph"]+"""> <http://www.w3.org/ns/prov#used> ?entities_to_check. 
            }
        """

        sparql.setQuery(query)
        response = sparql.queryAndConvert()
        for result in response["results"]["bindings"]:
            props_not_linked.remove(result["entities_to_check"]["value"])

    if len(props_not_linked):
        print(f"Warning : {len(props_not_linked)} properties are unknown in the keys.")
        with open("./listOfUnknownProperties.tsv", "w", encoding="UTF-8") as f_out:
            for p in props_not_linked:
                f_out.write(f"{p}\n")

        
    


    
    

