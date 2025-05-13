from SPARQLWrapper import SPARQLWrapper, BASIC
import json

import numpy as np
import pandas as pd

def retrieve_vocabularies():
    query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT distinct ?vocab
        WHERE {
            ?vocab rdf:type <http://purl.org/vocommons/voaf#Vocabulary>.
            ?smth rdfs:isDefinedBy ?vocab.
        }
    """ 
    vocabularies = list()
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        vocabularies.append(result["vocab"]["value"])
    return vocabularies

def retrieve_classes_from_vocab():
    query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        SELECT ?uri_class_1 ?uri_class_2 ?score ?vocab_1 ?vocab_2
        WHERE {
            ?uri_class_1 rdf:type rdfs:Class.
            ?uri_class_2 rdf:type rdfs:Class.

            ?uri_class_1 rdfs:isDefinedBy ?vocab_1.
            ?uri_class_2 rdfs:isDefinedBy ?vocab_2.
            
            FILTER (str(?vocab_1) > str(?vocab_2))
            {
                SELECT ?uri_class_1 ?uri_class_2 ?score 
                WHERE {
                    GRAPH ?score {
                        {
                            ?uri_class_1 <http://KG_Nexus.com/similarityScore> ?uri_class_2.
                        } UNION {
                            ?uri_class_2 <http://KG_Nexus.com/similarityScore> ?uri_class_1.
                        }
                    }
                }
            }
        }
    """
    
    similarities = dict()
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        vocab_1 = result["vocab_1"]["value"]
        vocab_2 = result["vocab_2"]["value"]
        if (vocab_1, vocab_2) not in similarities:
            similarities[(vocab_1, vocab_2)] = set()

        score_raw = result["score"]["value"].split("/")[-1]

        similarities[(vocab_1, vocab_2)].add((float(score_raw.split("_")[0]), \
                          float(score_raw.split("_")[1]),
                          result["uri_class_1"]["value"],\
                          result["uri_class_2"]["value"]))
        
    return similarities

def retrieve_properties_from_vocab():
    query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        SELECT ?uri_property_1 ?uri_property_2 ?score ?vocab_1 ?vocab_2
        WHERE {
            ?uri_property_1 rdf:type rdf:Property.
            ?uri_property_2 rdf:type rdf:Property.

            ?uri_property_1 rdfs:isDefinedBy ?vocab_1.
            ?uri_property_2 rdfs:isDefinedBy ?vocab_2.
            
            FILTER (str(?vocab_1) > str(?vocab_2))
            {
                SELECT ?uri_property_1 ?uri_property_2 ?score 
                WHERE {
                    GRAPH ?score {
                        {
                            ?uri_property_1 <http://KG_Nexus.com/similarityScore> ?uri_property_2.
                        } UNION {
                            ?uri_property_2 <http://KG_Nexus.com/similarityScore> ?uri_property_1.
                        }
                    }
                }
            }
        }
    """
    
    similarities = dict()
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        vocab_1 = result["vocab_1"]["value"]
        vocab_2 = result["vocab_2"]["value"]
        if (vocab_1, vocab_2) not in similarities:
            similarities[(vocab_1, vocab_2)] = set()

        score_raw = result["score"]["value"].split("/")[-1] 

        similarities[(vocab_1, vocab_2)].add((float(score_raw.split("_")[0]), \
                          float(score_raw.split("_")[1]),
                          result["uri_property_1"]["value"],\
                          result["uri_property_2"]["value"]))
        
    return similarities

def take_decision(similarities):

    def add_alignments(line, already_aligned, alignments):
        slack_allowed = 0.05
        if line[2] not in already_aligned and line[3] not in already_aligned:
            alignments.add((line[2], line[3]))
            already_aligned[line[2]] = line[0]
            already_aligned[line[3]] = line[0]

        elif (line[2] in already_aligned and already_aligned[line[2]] - line[0] < slack_allowed) and\
             (line[3] in already_aligned and already_aligned[line[3]] - line[0] < slack_allowed):
            alignments.add((line[2], line[3]))
            if line[2] not in already_aligned:
                already_aligned[line[2]] = line[0]

            if line[3] not in already_aligned:
                already_aligned[line[3]] = line[0]           

    already_aligned = dict()
    alignments = set()
    df_similarities = pd.DataFrame(similarities).sort_values(0, ascending=False)
    df_similarities.apply(lambda x: add_alignments(x, already_aligned, alignments), axis=1)

    return alignments


if __name__ == "__main__":
    config = json.load(open("config.json", "r"))

    url_server = config["URL_endpoint"]
    sparql = SPARQLWrapper(url_server)
    sparql.setReturnFormat('json')
    sparql.method = 'GET'


    with open("./equivalences.nt", "w", encoding="UTF-8") as f_out:

        alignments_decided = set()

        sim_classes = retrieve_classes_from_vocab()
        vocabs = list(sim_classes.keys())
        
        for couple in vocabs:
            similarities = sim_classes[couple]
            if len(similarities):
                alignments = take_decision(similarities)
                for c1, c2 in alignments:
                    f_out.write(f"<{c1}> <http://KG_Nexus.com/equivalenceClassComputed> <{c2}>.\n")

        sim_properties = retrieve_properties_from_vocab()
        vocabs = list(sim_properties.keys())
        
        for couple in vocabs:
            similarities = sim_properties[couple]
            if len(similarities):
                alignments = take_decision(similarities)
                for c1, c2 in alignments:
                    f_out.write(f"<{c1}> <http://KG_Nexus.com/equivalencePropertyComputed> <{c2}>.\n")
    
