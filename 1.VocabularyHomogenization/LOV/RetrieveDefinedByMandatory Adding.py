import pandas as pd
from SPARQLWrapper import SPARQLWrapper, BASIC
import re

url_server = "http://localhost:7200/repositories/LOV"

def curate_literal(string_to_curate:str) -> str:
    return re.sub("\s", " ", string_to_curate).replace('"',' ').replace('\\',' ')

def curate_value(value:dict)->str:
    valueRelation = ""
    if value["type"] == "literal": 
        valueRelation += f'"{curate_literal(value["""value"""])}"'
        if "xml:lang" in value:
            valueRelation += f'@{value["xml:lang"]}'
    else:
        valueRelation += f"<{value['value']}>"
    return valueRelation

def retrieve_vocabularies(sparql:SPARQLWrapper):
    query = """
        PREFIX vann:<http://purl.org/vocab/vann/>
        PREFIX voaf:<http://purl.org/vocommons/voaf#>
        
        ### Vocabularies contained in LOV and their prefix
        SELECT DISTINCT ?vocabURI {
            GRAPH <https://lov.linkeddata.es/dataset/lov>{
                ?vocabURI a voaf:Vocabulary.
        }}
    """

    vocabularies = dict()

    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        vocabularies[result["vocabURI"]["value"]] = dict()

    interesting_relations_to_retrieve = [
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://purl.org/dc/terms/modified",
        "http://purl.org/vocab/vann/preferredNamespacePrefix",
        "http://purl.org/vocab/vann/preferredNamespaceUri",
        "http://xmlns.com/foaf/0.1/homepage",
        # "http://purl.org/dc/terms/contributor",
        "http://purl.org/dc/terms/issued",
        "http://purl.org/dc/terms/publisher",
        "http://purl.org/dc/terms/title",
        "http://purl.org/dc/terms/description",
        # "http://purl.org/stuff/rev#hasReview",
        # "http://www.w3.org/ns/dcat#distribution",
        # "http://www.w3.org/ns/dcat#keyword",
        "http://purl.org/dc/terms/creator",
        # "http://purl.org/dc/terms/language",
        # "http://purl.org/vocommons/voaf#occurrencesInDatasets",
        # "http://purl.org/vocommons/voaf#reusedByDatasets",
        # "http://purl.org/vocommons/voaf#usageInDataset",
        "http://purl.org/vocommons/voaf#reusedByVocabularies",
        "http://www.w3.org/2000/01/rdf-schema#isDefinedBy",
        "http://www.w3.org/ns/dcat#keyword"
    ]
    for relation_to_retrieve in interesting_relations_to_retrieve:
        query = """
            PREFIX vann:<http://purl.org/vocab/vann/>
            PREFIX voaf:<http://purl.org/vocommons/voaf#>
            
            ### Vocabularies contained in LOV and their prefix
            SELECT DISTINCT ?vocabURI ?valueRelation {
                GRAPH <https://lov.linkeddata.es/dataset/lov>{
                    ?vocabURI a voaf:Vocabulary.
                    ?vocabURI <"""+relation_to_retrieve+"""> ?valueRelation.
                    FILTER(! isBlank(?valueRelation))
                    FILTER(! isBlank(?vocabURI))
            }}
        """

        sparql.setQuery(query)
        response = sparql.queryAndConvert()
        for result in response["results"]["bindings"]:

            if relation_to_retrieve == "http://purl.org/dc/terms/description":
                relation_to_retrieve = "http://www.w3.org/2000/01/rdf-schema#comment"

            if not relation_to_retrieve in vocabularies[result["vocabURI"]["value"]]:
                vocabularies[result["vocabURI"]["value"]][relation_to_retrieve] = set()
            
            valueRelation = curate_value(result["valueRelation"])
            # if result["valueRelation"]["type"] == "literal": 
            #     valueRelation = f'"{curate_literal(result["""valueRelation"""]["""value"""])}"'
            #     if "lang" in result["valueRelation"]:
            #         valueRelation += f'@{result["valueRelation"]["lang"]}'
            # else:
            #     valueRelation = f"<{result['valueRelation']['value']}>"

            vocabularies[result["vocabURI"]["value"]][relation_to_retrieve].add(valueRelation)
        


    return vocabularies
            
def retrieve_properties(sparql:SPARQLWrapper):
            
    properties = dict()
    
    query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX voaf:<http://purl.org/vocommons/voaf#>
        
        SELECT DISTINCT ?propURI ?classProperty {
            VALUES ?classProperty {
                    owl:DatatypeProperty
                    owl:ObjectProperty
                    rdf:Property
                }
            ?propURI rdf:type ?classProperty.            
            FILTER(! isBlank(?propURI))
        }
    """

    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        properties[result["propURI"]["value"]] = dict()

    interesting_relations_to_retrieve = {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" : set([
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "http://purl.org/dc/elements/1.1/type"
                ]),
        "http://www.w3.org/2000/01/rdf-schema#label" : set([
                    "http://www.w3.org/2000/01/rdf-schema#label",
                    "http://xmlns.com/foaf/spec/name",
                    "http://xmlns.com/foaf/0.1/name",
                    "http://purl.org/dc/elements/1.1/title",
                    "http://comicmeta.org/cbo/qlabel",
                ]),
        "http://www.w3.org/2000/01/rdf-schema#comment" : set([
                    "http://www.w3.org/2000/01/rdf-schema#comment",
                    "http://www.w3.org/2000/01/rdf-schema#commenet",
                    "http://purl.org/dc/elements/1.1/description",
                    "http://purl.org/dc/terms/description",
                    "http://www.w3.org/2004/02/skos/core#definition",
                    "http://vocab.gtfs.org/terms#comment",
                    "http://schema.org/comment",
                    "http://www.linkedmodel.org/schema/vaem#comment",
                    "http://www.w3.org/2000/01/rdf-schema#description",
                    "http://purl.org/imbi/ru-meta.owl#definition",
                    "http://www.w3.org/ns/prov#editorsDefinition",
                    "http://guava.iis.sinica.edu.tw/r4r/Definition",
                ]),
        "http://www.w3.org/2000/01/rdf-schema#domain" : set([
                    "http://www.w3.org/2000/01/rdf-schema#domain",
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#domain",
                ]),
        "http://www.w3.org/2000/01/rdf-schema#range" : set([
                    "http://www.w3.org/2000/01/rdf-schema#range",
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#range",
                    "http://www.w3.org/2000/01/rdf-schema#ramge",
                ]),
        "https://schema.org/domainIncludes":set([
                    "http://schema.org/domainIncludes",
                    "http://schema.org/#domainIncludes",
                    "https://www.schema.org/domainIncludes",
                    "https://schema.org/domainIncludes",
                    "https://ontologies.semanticarts.com/gist/domainIncludes",
                    "https://w3id.org/vocab/olca#domainIncludes",
                    "http://www.lingvoj.org/olca#domainIncludes",
                    "http://sparql.cwrc.ca/ontologies/cwrc#domainIncludes",
                
                ]),
        "https://schema.org/rangeIncludes":set([
                    "http://schema.org/rangeIncludes",
                    "https://schema.org/rangeIncludes",
                    "https://www.schema.org/rangeIncludes",
                    "http://sparql.cwrc.ca/ontologies/cwrc#rangeIncludes",
                    "https://w3id.org/vocab/olca#rangeIncludes",
                    "https://ontologies.semanticarts.com/gist/rangeIncludes",
                    "http://www.lingvoj.org/olca#rangeIncludes",
                ]),
        "http://www.w3.org/2000/01/rdf-schema#subPropertyOf":set([
                    "http://www.w3.org/2000/01/rdf-schema#subPropertyOf",
                    "http://www.w3.org/2002/07/owl#subPropertyOf",
                    "http://purl.oclc.org/NET/ssnx/ssn#subPropertyOf",
                    "http://www.w3.org/2000/01/rdf-schema#subPropertyof",
                    "http://www.w3.org/2002/07/owl#SubObjectPropertyOf",
                ]),
        "http://www.w3.org/2002/07/owl#equivalentProperty":set([
                    "http://www.w3.org/2002/07/owl#equivalentProperty",
                    "http://www.w3.org/2002/07/owl#sameAs",
                    "http://www.w3.org/2004/02/skos/core#equivalentProperty",
                    "http://semanticscience.org/resource/equivalentTo",
                ]),
        "http://www.w3.org/2002/07/owl#differentFrom":set([
                    "http://www.w3.org/2002/07/owl#differentFrom",
                ]),
        "http://www.w3.org/2002/07/owl#inverseOf":set([
                    "http://www.w3.org/2002/07/owl#inverseOf",
                    "http://www.w3.org/2002/07/owl#inverse",
                    "https://d-nb.info/standards/elementset/agrelon#correspondsToInverse",
                    "http://schema.org/inverseOf",
                    "http://www.w3.org/ns/prov#inverse",
                ]),
        "http://www.w3.org/2000/01/rdf-schema#isDefinedBy":set([
                    "http://www.w3.org/2000/01/rdf-schema#isDefinedBy",
                    "http://www.w3.org/2000/01/rdf-schema#isdefinedby"
                    # "http://www.w3.org/2007/05/powder-s#describedby"
        ])
    }
    
    for relation_to_retrieve in interesting_relations_to_retrieve:
        query = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX voaf:<http://purl.org/vocommons/voaf#>
            
            SELECT DISTINCT ?propURI ?valueRelation {
                VALUES ?classProperty {
                        owl:DatatypeProperty
                        owl:ObjectProperty
                        rdf:Property
                    }
                VALUES ?relationToRetrieve{
                    <"""+"> <".join(interesting_relations_to_retrieve[relation_to_retrieve])+""">
                }
                ?propURI rdf:type ?classProperty.
                ?propURI ?relationToRetrieve ?valueRelation.
                FILTER(! isBlank(?valueRelation))
                FILTER(! isBlank(?propURI))

                ?propURI rdfs:isDefinedBy ?vocab.
    			?vocab a ?vocabType
                VALUES ?vocabType {
                    owl:Ontology voaf:Vocabulary
                }

            }
        """

        sparql.setQuery(query)
        response = sparql.queryAndConvert()
        for result in response["results"]["bindings"]:
            if not relation_to_retrieve in \
                    properties[result["propURI"]["value"]]:
                properties[result["propURI"]["value"]][relation_to_retrieve] = set()
            
            valueRelation = curate_value(result["valueRelation"])

            properties[result["propURI"]["value"]][relation_to_retrieve].add(valueRelation)

    return properties

def retrieve_classes(sparql:SPARQLWrapper):
                
    classes = dict()
    
    query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX voaf:<http://purl.org/vocommons/voaf#>
        
        SELECT DISTINCT ?classURI {
            VALUES ?classClasses {
                    rdfs:Class
                    owl:Class
                }
            ?classURI rdf:type ?classClasses.
            FILTER(! isBlank(?classURI))
        }
    """    

    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        classes[result["classURI"]["value"]] = dict()

    interesting_relations_to_retrieve = {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" : set([
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "http://purl.org/dc/elements/1.1/type",
                ]),
        "http://www.w3.org/2000/01/rdf-schema#label" : set([
                    "http://www.w3.org/2000/01/rdf-schema#label",
                    "http://xmlns.com/foaf/spec/name",
                    "http://xmlns.com/foaf/0.1/name",
                    "http://purl.org/dc/elements/1.1/title",
                    "http://comicmeta.org/cbo/qlabel",
                ]),
        "http://www.w3.org/2000/01/rdf-schema#comment" : set([
                    "http://www.w3.org/2000/01/rdf-schema#comment",
                    "http://www.w3.org/2000/01/rdf-schema#commenet",
                    "http://purl.org/dc/elements/1.1/description",
                    "http://purl.org/dc/terms/description",
                    "http://www.w3.org/2004/02/skos/core#definition",
                    "http://vocab.gtfs.org/terms#comment",
                    "http://schema.org/comment",
                    "http://www.linkedmodel.org/schema/vaem#comment",
                    "http://www.w3.org/2000/01/rdf-schema#description",
                    "http://purl.org/imbi/ru-meta.owl#definition",
                    "http://www.w3.org/ns/prov#editorsDefinition",
                    "http://guava.iis.sinica.edu.tw/r4r/Definition",
                    "http://www.linkedmodel.org/schema/vaem#description"
                ]),
        "http://www.w3.org/2000/01/rdf-schema#subClassOf":set([
                    "http://www.w3.org/2000/01/rdf-schema#subClassOf",
                    "http://www.w3.org/2000/01/rdf-schema#subClasssOf",
                    "http://www.w3.org/2000/01/rdf-schema#subclassOf"
                ]),
        "http://www.w3.org/2002/07/owl#equivalentClass":set([
                    "http://www.w3.org/2002/07/owl#equivalentClass",
                ]),
        "http://www.w3.org/2002/07/owl#differentFrom":set([
                    "http://www.w3.org/2002/07/owl#differentFrom",
                ]),
        "http://www.w3.org/2000/01/rdf-schema#isDefinedBy":set([
                    "http://www.w3.org/2000/01/rdf-schema#isDefinedBy",
                    "http://www.w3.org/2000/01/rdf-schema#isdefinedby"
                    # "http://www.w3.org/2007/05/powder-s#describedby"
        ])
    }     

    for relation_to_retrieve in interesting_relations_to_retrieve:
        query = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX voaf:<http://purl.org/vocommons/voaf#>
            
            SELECT DISTINCT ?classURI ?valueRelation {
                VALUES ?classClasses {
                        rdfs:Class
                        owl:Class
                    }
                VALUES ?relationToRetrieve{
                    <"""+"> <".join(interesting_relations_to_retrieve[relation_to_retrieve])+""">
                }
                ?classURI rdf:type ?classClasses.
                ?classURI ?relationToRetrieve ?valueRelation.
                FILTER(! isBlank(?valueRelation))
                FILTER(! isBlank(?classURI))

                ?classURI rdfs:isDefinedBy ?vocab.
    			?vocab a ?vocabType
                VALUES ?vocabType {
                    owl:Ontology voaf:Vocabulary
                }
            }
        """

        sparql.setQuery(query)
        response = sparql.queryAndConvert()
        for result in response["results"]["bindings"]:
            if not relation_to_retrieve in \
                    classes[result["classURI"]["value"]]:
                classes[result["classURI"]["value"]][relation_to_retrieve] = set()
            
            valueRelation = curate_value(result["valueRelation"])

            classes[result["classURI"]["value"]][relation_to_retrieve].add(valueRelation)
    
    return classes

def write_data(f, vocabularies:dict, properties:dict, classes:dict):
    
    for vocabulary in vocabularies:

        ### Vocabulary meta data
        for property_vocabulary in set(vocabularies[vocabulary].keys()):
            for value in vocabularies[vocabulary][property_vocabulary]:
                f.write(f"<{vocabulary}> <{property_vocabulary}> {value}.\n")
        
    for property in properties:
        # f.write(f"<{property}> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <{vocabulary}>.\n")
        f.write(f"<{property}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>.\n")

        for property_property in properties[property]:
            for value in properties[property][property_property]:
                f.write(f"<{property}> <{property_property}> {value}.\n")

    for classe in classes:
        # f.write(f"<{class_voc}> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <{vocabulary}>.\n")
        f.write(f"<{classe}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Class>.\n")

        for property_class in classes[classe]:
            for value in classes[classe][property_class]:
                f.write(f"<{classe}> <{property_class}> {value}.\n")

def adding_isDefinedBy(sparql:SPARQLWrapper, components):

    components_to_check = [k for k in components.keys() if "http://www.w3.org/2000/01/rdf-schema#isDefinedBy" not in components[k]]
    step = 50
    vocabularies_components = dict()
    for i in range(0,len(components_to_check), step):

        query = """SELECT distinct ?uri ?v
                    WHERE {
                        graph ?v {
                            VALUES ?uri {
                            <"""+"> <".join(components_to_check[i:i+step])+""">
                            }
                            ?uri ?r ?l.
                        }
                    }"""
        print(query)
        sparql.setQuery(query)
        response = sparql.queryAndConvert()
        for result in response["results"]["bindings"]:
            uri = result["uri"]["value"]
            vocabulary = result["v"]["value"]
            if not uri in vocabularies_components:
                vocabularies_components[uri] = set()
            vocabularies_components[uri].add(f"<{vocabulary}>")
    
        for uri in vocabularies_components:
            if len(vocabularies_components[uri]) == 1:
                components[uri]["http://www.w3.org/2000/01/rdf-schema#isDefinedBy"] = list(vocabularies_components[uri])

def remove_not_isDefinedBy(components):
    for k in list(components.keys()):
        if "http://www.w3.org/2000/01/rdf-schema#isDefinedBy" not in components[k]:
            del components[k]


if __name__ == "__main__":

    sparql = SPARQLWrapper(url_server)
    sparql.setReturnFormat('json')
    sparql.method = 'GET'

    vocabularies = retrieve_vocabularies(sparql)

    properties = retrieve_properties(sparql)
    adding_isDefinedBy(sparql, properties)
    # remove_not_isDefinedBy(properties)

    classes = retrieve_classes(sparql)
    adding_isDefinedBy(sparql, classes)
    # remove_not_isDefinedBy(classes)

    with open("./HomogenizedDataIsDefinedMandatoryButImproved.nt", "w", encoding="UTF-8") as f:
        write_data(f, vocabularies, properties, classes)
        