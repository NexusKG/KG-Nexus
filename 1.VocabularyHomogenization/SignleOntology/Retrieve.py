import pandas as pd
from rdflib import Graph
import rdflib
import re
import sys

def curate_literal(string_to_curate:str) -> str:
    return re.sub("\s", " ", string_to_curate).replace('"',' ').replace('\\',' ')

def retrieve_vocabularies(graph, data):

    data["Vocabulary"] = dict()

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
    ]
    for relation_to_retrieve in interesting_relations_to_retrieve:
        query = """
            PREFIX vann:<http://purl.org/vocab/vann/>
            PREFIX voaf:<http://purl.org/vocommons/voaf#>
            PREFIX owl:<http://www.w3.org/2002/07/owl#>
            
            ### Vocabularies contained in LOV and their prefix
            SELECT DISTINCT ?vocabURI ?valueRelation {
                    VALUES ?classVocab {
                        voaf:Vocabulary
                        owl:Ontology 
                    }
                    ?vocabURI a ?classVocab.
                    ?vocabURI <"""+relation_to_retrieve+"""> ?valueRelation.
                    FILTER(! isBlank(?valueRelation))
                    FILTER(! isBlank(?vocabURI))
            }
        """
        
        for result in graph.query(query):

            if relation_to_retrieve == "http://purl.org/dc/terms/description":
                relation_to_retrieve = "http://www.w3.org/2000/01/rdf-schema#comment"

            data["VocabularyURI"] = result.vocabURI

            if not relation_to_retrieve in data["Vocabulary"]:
                data["Vocabulary"][relation_to_retrieve] = set()
            
            valueRelation = ""
            if type(result.valueRelation) == rdflib.term.Literal: 
                valueRelation = f'"{curate_literal(result.valueRelation.value)}"'
                if result.valueRelation.language:
                    valueRelation += f'@{result.valueRelation.language}'
            else:
                valueRelation = f"<{result.valueRelation}>"

            data["Vocabulary"][relation_to_retrieve].add(valueRelation)

def retrieve_properties(graph, data:dict):
            
    data["Property"] = dict()
    
    query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        
        SELECT DISTINCT ?propURI {
            VALUES ?classProperty {
                    owl:DatatypeProperty
                    owl:ObjectProperty
                    rdf:Property
                }
            ?propURI rdf:type ?classProperty.
            FILTER(! isBlank(?propURI))
        }
    """

    for result in graph.query(query):
        data["Property"][result.propURI] = dict()

    interesting_relations_to_retrieve = {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" : set([
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
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
        "http://www.w3.org/ns/prov#category":set([
                    "http://www.w3.org/ns/prov#category",
        ]),
        "http://www.w3.org/2000/01/rdf-schema#isDefinedBy":set([
                    "http://www.w3.org/2000/01/rdf-schema#isDefinedBy"
                    "http://www.w3.org/2007/05/powder-s#describedby"
        ])
    }
    
    for relation_to_retrieve in interesting_relations_to_retrieve:
        query = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            
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
                FILTER(! isBlank(?propURI))
                FILTER(! isBlank(?valueRelation))

            }
        """
        for result in graph.query(query):
            if not relation_to_retrieve in \
                    data["Property"][result.propURI]:
                data["Property"][result.propURI][relation_to_retrieve] = set()
            
            valueRelation = ""
            if type(result.valueRelation) == rdflib.term.Literal: 
                valueRelation = f'"{curate_literal(result.valueRelation.value)}"'
                if result.valueRelation.language:
                    valueRelation += f'@{result.valueRelation.language}'
            else:
                valueRelation = f"<{result.valueRelation}>"

            data["Property"][result.propURI][relation_to_retrieve].add(valueRelation)

def retrieve_classes(graph, data:dict):
                
        data["Class"] = dict()
        
        query = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            
            SELECT DISTINCT ?classURI {
                VALUES ?classClasses {
                        rdfs:Class
                    }
                ?classURI rdf:type ?classClasses.
                FILTER(! isBlank(?classURI))
            }
        """    

        for result in graph.query(query):
            data["Class"][result.classURI] = dict()

        interesting_relations_to_retrieve = {
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" : set([
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
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
            "http://www.w3.org/ns/prov#category":set([
                        "http://www.w3.org/ns/prov#category",
            ]),
            "http://www.w3.org/2000/01/rdf-schema#isDefinedBy":set([
                        "http://www.w3.org/2000/01/rdf-schema#isDefinedBy"
                        "http://www.w3.org/2007/05/powder-s#describedby"
            ])
        }     

        for relation_to_retrieve in interesting_relations_to_retrieve:
            query = """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                
                SELECT DISTINCT ?classURI ?valueRelation {
                    VALUES ?classClasses {
                            rdfs:Class
                        }
                    VALUES ?relationToRetrieve{
                        <"""+"> <".join(interesting_relations_to_retrieve[relation_to_retrieve])+""">
                    }
                    ?classURI rdf:type ?classClasses.
                    ?classURI ?relationToRetrieve ?valueRelation.
                    FILTER(! isBlank(?classURI))
                    FILTER(! isBlank(?valueRelation))

                }
            """
            
            for result in graph.query(query):
                if not relation_to_retrieve in \
                        data["Class"][result.classURI]:
                    data["Class"][result.classURI][relation_to_retrieve] = set()
                
                valueRelation = ""
                if type(result.valueRelation) == "literal": 
                    valueRelation = f'"{curate_literal(result.valueRelation.value)}"'
                    if result.valueRelation.language:
                        valueRelation += f'@{result.valueRelation.language}'
                else:
                    valueRelation = f"<{result.valueRelation}>"

                data["Class"][result.classURI][relation_to_retrieve].add(valueRelation)

def write_data(f, data:dict):

    ### Vocabulary meta data
    vocabulary = data["VocabularyURI"]
    for property_vocabulary in set(data["Vocabulary"]):
        for value in data["Vocabulary"][property_vocabulary]:
            f.write(f"<{vocabulary}> <{property_vocabulary}> {value}.\n")
    
    for property in data["Property"]:
        f.write(f"<{property}> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <{vocabulary}>.\n")
        f.write(f"<{property}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>.\n")

        for property_property in data["Property"][property]:
            for value in data['Property'][property][property_property]:
                f.write(f"<{property}> <{property_property}> {value}.\n")

    for class_voc in data["Class"]:
        f.write(f"<{class_voc}> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <{vocabulary}>.\n")
        f.write(f"<{class_voc}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Class>.\n")

        for property_class in data["Class"][class_voc]:
            for value in data['Class'][class_voc][property_class]:
                f.write(f"<{class_voc}> <{property_class}> {value}.\n")

if __name__ == "__main__":

    graph = Graph()
    graph.parse(sys.argv[1])
    
    data = {}
    retrieve_vocabularies(graph, data)
    retrieve_properties(graph, data)
    retrieve_classes(graph, data)

    with open(f"./HomogenizedData_{sys.argv[1].split('.')[0]}.nt", "w", encoding="UTF-8") as f:
        write_data(f, data)
        