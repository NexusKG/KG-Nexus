import re
import ijson 
from subprocess import DEVNULL, STDOUT, check_call

SPARQL_PATH = "./Sparql.jar"
WIKIDATA_PATH = "./../Graphs_HDT/Wikidata/Wikidata_final.hdt"

def query_wikidata(query):
    with open("./query.txt", "w", encoding="UTF-8") as f_w:
        f_w.write(query)

    cmd = f"java -jar {SPARQL_PATH} {WIKIDATA_PATH} query.txt ./Result_query.json"
    check_call(cmd, shell=True, stdout=DEVNULL, stderr=STDOUT) 

def clean():
    cmd = f"rm ./query.txt ./Result_query.json"
    check_call(cmd, shell=True, stdout=DEVNULL, stderr=STDOUT)

def curate_literal(string_to_curate:str) -> str:
    return re.sub("\s", " ", string_to_curate).replace('"',' ').replace('\\',' ')

def retrieve_vocabularies(f_out, data):
    data["VocabularyIRI"] = "http://wikidata.org/"
    f_out.write(f'<{data["VocabularyIRI"]}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/vocommons/voaf#Vocabulary>.\n')
    f_out.write(f'<{data["VocabularyIRI"]}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Ontology>.\n')
    f_out.write(f'<{data["VocabularyIRI"]}> <http://purl.org/dc/terms/modified> "2023-12-01T12:00Z".\n')
    f_out.write(f'<{data["VocabularyIRI"]}> <http://xmlns.com/foaf/0.1/homepage> "http://wikidata.org/".\n')
    f_out.write(f'<{data["VocabularyIRI"]}> <http://purl.org/dc/terms/modified> "2023-12-01T12:00Z".\n')
    f_out.write(f'<{data["VocabularyIRI"]}> <http://purl.org/dc/terms/title> "Wikidata"@en.\n')
    f_out.write(f'<{data["VocabularyIRI"]}> <http://www.w3.org/2000/01/rdf-schema#comment> "free knowledge graph hosted by Wikimedia and edited by volunteers"@en.\n')
    f_out.write(f'<{data["VocabularyIRI"]}> <http://purl.org/vocab/vann/preferredNamespacePrefix> "wd".\n')

def retrieve_properties(f_out, data):

    query = """
    SELECT DISTINCT ?property ?type
    WHERE {
        ?property <http://wikiba.se/ontology#propertyType> ?type.
        FILTER(! isBlank(?property))
    }"""
    query_wikidata(query)

    with open(f"Result_query.json", 'r', encoding="UTF-8") as f_read:
        for record in ijson.items(f_read, "results.bindings.item"):
            property = record["property"]["value"]

            id_prop = property.split("/")[-1]

            prefixes = ["http://www.wikidata.org/prop/direct/", "http://www.wikidata.org/prop/direct-normalized/", "http://www.wikidata.org/prop/statement/", "http://www.wikidata.org/prop/statement/value/", "http://www.wikidata.org/prop/statement/value-normalized/"]
            for prefixe in prefixes:
                f_out.write(f'<{property}> <http://www.graph/alternativeProp> <{prefixe}{id_prop}>.\n')

            type = record["type"]["value"]

            f_out.write(f'<{property}> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <{data["VocabularyIRI"]}>.\n')
            f_out.write(f'<{property}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{type}>.\n')
            f_out.write(f'<{property}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>.\n')
            


    #######

    query = """
    SELECT DISTINCT ?property ?label ?description
    WHERE {
        ?property <http://wikiba.se/ontology#propertyType> ?type.
        ?property <http://www.w3.org/2000/01/rdf-schema#label> ?label.
        OPTIONAL{
            ?property <http://schema.org/description> ?description.
            FILTER (lang(?label) = lang(?description))
            FILTER(! isBlank(?property))
            FILTER(! isBlank(?description))
        }
    }"""
    query_wikidata(query)
    with open(f"Result_query.json", 'r', encoding="UTF-8") as f_read:
        for record in ijson.items(f_read, "results.bindings.item"):
            property_iri = record["property"]["value"]

            label = curate_literal(str(record["label"]["value"]))
            label_lang = record["label"]["xml:lang"]
            f_out.write(f'<{property_iri}> <http://www.w3.org/2000/01/rdf-schema#label> "{label}"@{label_lang}.\n')


            if "description" in record:
                descript = curate_literal(str(record["description"]["value"]))
                descript_lang = record["description"]["xml:lang"]
                f_out.write(f'<{property_iri}> <http://www.w3.org/2000/01/rdf-schema#comment> "{descript}"@{descript_lang}.\n')
        

    #######

    query = """
    SELECT DISTINCT ?property ?domain
    WHERE {
        ?property <http://wikiba.se/ontology#propertyType> ?type.
        ?property <http://www.wikidata.org/prop/P2302> ?v.
        ?v <http://www.wikidata.org/prop/statement/P2302> <http://www.wikidata.org/entity/Q21503250>.
        ?v <http://www.wikidata.org/prop/qualifier/P2308> ?domain.

        FILTER(! isBlank(?property))
        FILTER(! isBlank(?domain))
    }"""
    query_wikidata(query)
    with open(f"Result_query.json", 'r', encoding="UTF-8") as f_read:
        for record in ijson.items(f_read, "results.bindings.item"):
            property_iri = record["property"]["value"]

            domain = record["domain"]["value"]

            f_out.write(f'<{property_iri}> <https://schema.org/domainIncludes> <{domain}>.\n')

    #######

    query = """
    SELECT DISTINCT ?property ?range
    WHERE {
        ?property <http://wikiba.se/ontology#propertyType> ?type. 
        ?property <http://www.wikidata.org/prop/P2302> ?statement.
        ?statement <http://www.wikidata.org/prop/statement/P2302> <http://www.wikidata.org/entity/Q21510865>.
        ?statement <http://www.wikidata.org/prop/qualifier/P2308> ?range
            FILTER(! isBlank(?property))
            FILTER(! isBlank(?range))
    }"""
    query_wikidata(query)
    with open(f"Result_query.json", 'r', encoding="UTF-8") as f_read:
        for record in ijson.items(f_read, "results.bindings.item"):
            property_iri = record["property"]["value"]

            range = record["range"]["value"]

            f_out.write(f'<{property_iri}> <https://schema.org/rangeIncludes> <{range}>.\n')


    #######

    query = """
    SELECT DISTINCT ?property ?sup_prop
    WHERE {
        ?property <http://wikiba.se/ontology#propertyType> ?type.
        ?property <http://www.wikidata.org/prop/direct/P1647> ?sup_prop.
        FILTER(! isBlank(?property))
        FILTER(! isBlank(?sup_prop))
    }"""
    query_wikidata(query)
    with open(f"Result_query.json", 'r', encoding="UTF-8") as f_read:
        for record in ijson.items(f_read, "results.bindings.item"):
        
            prop_iri = record["property"]["value"]
            sup_prop = record["sup_prop"]["value"]

            f_out.write(f"<{prop_iri}> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <{sup_prop}>.\n")

    ########

    query = """
    SELECT DISTINCT ?property ?ext_prop
    WHERE {
        ?property <http://wikiba.se/ontology#propertyType> ?type.
        ?property <http://www.wikidata.org/prop/direct/P1628> ?ext_prop.
        FILTER(! isBlank(?property))
        FILTER(! isBlank(?ext_prop))
    }"""
    query_wikidata(query)
    with open(f"Result_query.json", 'r', encoding="UTF-8") as f_read:
        for record in ijson.items(f_read, "results.bindings.item"):
        
            prop_iri = record["property"]["value"]
            ext_prop = record["ext_prop"]["value"]

            f_out.write(f"<{prop_iri}> <http://www.w3.org/2002/07/owl#equivalentProperty> <{ext_prop}>.\n")

def retrieve_classes(f_out, data):
    data["Class"] = list()
    query = """
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        SELECT DISTINCT ?class
        WHERE {
            ?entity wdt:P31 ?class.
            FILTER(! isBlank(?class))
        }
        """
    query_wikidata(query)
    with open(f"Result_query.json", 'r', encoding="UTF-8") as f_read:
        for record in ijson.items(f_read, "results.bindings.item"):
            class_wikidata = record["class"]["value"]

            f_out.write(f'<{class_wikidata}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Class>.\n')
            f_out.write(f'<{class_wikidata}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class>.\n')
            f_out.write(f'<{class_wikidata}> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <{data["VocabularyIRI"]}>.\n')
            data["Class"].append(class_wikidata)
    
    step = 1000
    for i in range(0, len(data["Class"]), step):

        query = """
                SELECT DISTINCT ?class ?c_sub
                WHERE {
                VALUES ?class { <"""+"> <".join(data["Class"][i:i+step])+"""> }
                ?class <http://www.wikidata.org/prop/direct/P279> ?c_sub.
                FILTER(! isBlank(?c_sub))
            } """
        query_wikidata(query)
        with open(f"Result_query.json", 'r', encoding="UTF-8") as f_read:
            for record in ijson.items(f_read, "results.bindings.item"):
                class_sub = record["class"]["value"]
                class_sup = record["c_sub"]["value"]
                f_out.write(f"<{class_sub}> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <{class_sup}>.\n")


        query = """
            SELECT DISTINCT ?class ?label ?description
            WHERE {
            VALUES ?class { <"""+"> <".join(data["Class"][i:i+step])+"""> }
            ?class <http://www.w3.org/2000/01/rdf-schema#label> ?label.
                FILTER(! isBlank(?label))
            OPTIONAL{
                ?class <http://schema.org/description> ?description.
                FILTER (lang(?label) = lang(?description))
            }
            } """
        query_wikidata(query)
        with open(f"Result_query.json", 'r', encoding="UTF-8") as f_read:
            for record in ijson.items(f_read, "results.bindings.item"):
                class_iri = record["class"]["value"]

                label = curate_literal(str(record["label"]["value"]))
                label_lang = record["label"]["xml:lang"]
                f_out.write(f'<{class_iri}> <http://www.w3.org/2000/01/rdf-schema#label> "{label}"@{label_lang}.\n')


                if "description" in record:

                    descript = curate_literal(str(record["description"]["value"]))
                    descript_lang = record["description"]["xml:lang"]
                    f_out.write(f'<{class_iri}> <http://www.w3.org/2000/01/rdf-schema#comment> "{descript}"@{descript_lang}.\n')


        query = """
            SELECT DISTINCT ?class ?equivalence
            WHERE { 
            VALUES ?class { <"""+"> <".join(data["Class"][i:i+step])+"""> }
            ?class <http://www.wikidata.org/prop/direct/P1709> ?equivalence.
                FILTER(! isBlank(?equivalence))
            
            }
        """
        query_wikidata(query)
        with open(f"Result_query.json", 'r', encoding="UTF-8") as f_read:
            for record in ijson.items(f_read, "results.bindings.item"):
                class_iri = record["class"]["value"]
                equivalence = record["equivalence"]["value"]

                f_out.write(f"<{class_iri}> <http://www.w3.org/2002/07/owl#equivalentClass> <{equivalence}>.\n")


if __name__ == "__main__":
    
    data = {}

    with open(f"./HomogenizedData_Wikidata.nt", "w", encoding="UTF-8") as f:
        retrieve_vocabularies(f, data)
        retrieve_properties(f, data)
        retrieve_classes(f, data)
        clean()
        