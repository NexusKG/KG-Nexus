from SPARQLWrapper import SPARQLWrapper, BASIC
import json
from absl import logging
import tensorflow as tf
import tensorflow_hub as hub
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re
import seaborn as sns
from sklearn.metrics.pairwise import cosine_similarity

def retrieve_vocabularies():
    query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT distinct ?vocab
        WHERE {
            ?vocab rdf:type <http://purl.org/vocommons/voaf#Vocabulary>
        }
    """ 
    vocabularies = set()
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        vocabularies.add(result["vocab"]["value"])
    return vocabularies

def retrieve_classes_from_n_vocab(vocabularies, languages):
    classes_per_vocab = dict()
    for vocabulary in vocabularies:
        classes_per_vocab[vocabulary] = retrieve_classes_from_vocab(vocabulary, languages)
    return classes_per_vocab

def retrieve_classes_from_vocab(uri_vocab, languages):
    query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        SELECT ?uri_class ?label ?comment
        WHERE {
            ?uri_class rdfs:isDefinedBy <"""+uri_vocab+""">;
                    rdf:type rdfs:Class;
                    rdfs:label ?label.
            FILTER(lang(?label) in ('"""+"', '".join(languages)+"""'))
            OPTIONAL{
                ?uri_class rdfs:comment ?comment.
                FILTER(lang(?comment) = lang(?label))
            }
        }
        """
    
    classes = dict()
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:

        uri = result["uri_class"]["value"]
        label = result["label"]
        comment = None
        if "comment" in result:
            comment = result["comment"]

        if uri not in classes :
            classes[uri] = {
                "label":dict(),
                "comment":dict()
            }

        classes[uri]["label"][label["xml:lang"]] = label["value"]

        if comment:
            classes[uri]["comment"][comment["xml:lang"]] = comment["value"]
    
    return classes

def retrieve_properties_from_n_vocab(vocabularies, languages):
    properties_per_vocab = dict()
    for vocabulary in vocabularies:
        properties_per_vocab[vocabulary] = retrieve_properties_from_vocab(vocabulary, languages)
    return properties_per_vocab

def retrieve_properties_from_vocab(uri_vocab, languages):
    query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        SELECT ?uri_prop ?label ?comment
        WHERE {
            ?uri_prop rdfs:isDefinedBy <"""+uri_vocab+""">;
                    rdf:type rdf:Property;
                    rdfs:label ?label;
            FILTER(lang(?label) in ('"""+"', '".join(languages)+"""'))
            OPTIONAL{
                ?uri_prop rdfs:comment ?comment.
                FILTER(lang(?comment) = lang(?label))
            }
        }
        """
    
    properties = dict()
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:

        uri = result["uri_prop"]["value"]
        label = result["label"]
        comment = None
        if "comment" in result:
            comment = result["comment"]

        if uri not in properties :
            properties[uri] = {
                "label":dict(),
                "comment":dict(), 
                "domain":set(),
                "range":set(),
                "domainIncludes":set(),
                "rangeIncludes":set()
            }

        properties[uri]["label"][label["xml:lang"]] = label["value"]

        if comment:
            properties[uri]["comment"][comment["xml:lang"]] = comment["value"]

    query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        SELECT distinct ?uri_prop ?domain
        WHERE {
            ?uri_prop rdfs:isDefinedBy <"""+uri_vocab+""">;
                    rdf:type rdf:Property;
                    rdfs:domain ?domain.          
        }
        """
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        properties[uri]["domain"].add(result["domain"]["value"])
    
    query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        SELECT distinct ?uri_prop ?range
        WHERE {
            ?uri_prop rdfs:isDefinedBy <"""+uri_vocab+""">;
                    rdf:type rdf:Property;
                    rdfs:range ?range.      
        }
        """
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        properties[uri]["range"].add(result["range"]["value"])
    
    query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        SELECT distinct ?uri_prop ?domainIncludes
        WHERE {
            ?uri_prop rdfs:isDefinedBy <"""+uri_vocab+""">;
                    rdf:type rdf:Property;
                    <https://schema.org/domainIncludes> ?domainIncludes.      
        }
        """
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        properties[uri]["domainIncludes"].add(result["domainIncludes"]["value"])
    
    query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        SELECT distinct ?uri_prop ?rangeIncludes
        WHERE {
            ?uri_prop rdfs:isDefinedBy <"""+uri_vocab+""">;
                    rdf:type rdf:Property;
                    <https://schema.org/rangeIncludes> ?rangeIncludes.      
        }
        """
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        properties[uri]["rangeIncludes"].add(result["rangeIncludes"]["value"])

    return properties

def compute_similarity_classes(data_1, data_2, language="en"):
    keys_1 = list(data_1.keys())
    data_1_labels_embedding = embed([data_1[key]["label"][language] if 'en' in data_1[key]["label"] else "" for key in keys_1])
    classes_1_with_no_comments = set([-1 if 'en' in data_1[key]["comment"] else i for i, key in enumerate(keys_1)])
    data_1_comments_embedding = embed([data_1[key]["comment"][language] if 'en' in data_1[key]["comment"] else "" for key in keys_1])

    keys_2 = list(data_2.keys())
    data_2_labels_embedding = embed([data_2[key]["label"][language] if 'en' in data_2[key]["label"] else "" for key in keys_2])
    classes_2_with_no_comments = set([-1 if 'en' in data_2[key]["comment"] else i for i, key in enumerate(keys_2)])
    data_2_comments_embedding = embed([data_2[key]["comment"][language] if 'en' in data_2[key]["comment"] else "" for key in keys_2  ])

    cosine_similarity_labels = cosine_similarity(data_1_labels_embedding, data_2_labels_embedding)
    cosine_similarity_comments = cosine_similarity(data_1_comments_embedding, data_2_comments_embedding)
    
    flatten_cosine_similarity_labels = cosine_similarity_labels.flatten()
    flatten_cosine_similarity_comments = cosine_similarity_comments.flatten() 
    flatten_cosine_similarity_average = np.array([np.average([cosine_similarity_labels[i][j], cosine_similarity_comments[i][j]]) if i not in classes_1_with_no_comments and j not in classes_2_with_no_comments else cosine_similarity_labels[i][j]  for i in range(len(keys_1)) for j in range(len(keys_2))]).flatten()
    comment_used = [i not in classes_1_with_no_comments and j not in classes_2_with_no_comments  for i in range(len(keys_1)) for j in range(len(keys_2))]
    
    df = pd.DataFrame(data={
                                "Label":flatten_cosine_similarity_labels, 
                                "Comment":flatten_cosine_similarity_comments, 
                                "Average":flatten_cosine_similarity_average,
                                "Comment Used":comment_used
                            }, 
                      index=[str((i,j)) for i in keys_1 for j in keys_2])

    return df

def find_best_classes_sim(similarity_classes, classes_1, classes_2):

    best_value = -10
    for class_1 in classes_1:
        for class_2 in classes_2:
            if str((class_1, class_2)) in similarity_classes.index:
                best_value = max(best_value, similarity_classes.loc[str((class_1, class_2))]["Average"])

    return best_value

def compute_similarity_properties(data_1, data_2, similarity_classes, language="en"):
    keys_1 = list(data_1.keys())
    data_1_labels_embedding = embed([data_1[key]["label"][language] if 'en' in data_1[key]["label"] else "" for key in keys_1])
    classes_1_with_no_comments = set([-1 if 'en' in data_1[key]["comment"] else i for i, key in enumerate(keys_1)])
    data_1_comments_embedding = embed([data_1[key]["comment"][language] if 'en' in data_1[key]["comment"] else "" for key in keys_1])
    classes_1_domain_domainIncludes = {key:data_1[key]["domain"].union(data_1[key]["domainIncludes"]) for key in keys_1}
    classes_1_range_rangeIncludes = {key:data_1[key]["range"].union(data_1[key]["rangeIncludes"]) for key in keys_1}

    keys_2 = list(data_2.keys())
    data_2_labels_embedding = embed([data_2[key]["label"][language] if 'en' in data_2[key]["label"] else "" for key in keys_2])
    classes_2_with_no_comments = set([-1 if 'en' in data_2[key]["comment"] else i for i, key in enumerate(keys_2)])
    data_2_comments_embedding = embed([data_2[key]["comment"][language] if 'en' in data_2[key]["comment"] else "" for key in keys_2  ])
    classes_2_domain_domainIncludes = {key:data_2[key]["domain"].union(data_2[key]["domainIncludes"]) for key in keys_2}
    classes_2_range_rangeIncludes = {key:data_2[key]["range"].union(data_2[key]["rangeIncludes"]) for key in keys_2}


    cosine_similarity_labels = cosine_similarity(data_1_labels_embedding, data_2_labels_embedding)
    cosine_similarity_comments = cosine_similarity(data_1_comments_embedding, data_2_comments_embedding)
    cosine_similarity_domain = [find_best_classes_sim(similarity_classes, classes_1_domain_domainIncludes[key_1], classes_2_domain_domainIncludes[key_2]) for key_1 in keys_1 for key_2 in keys_2]
    cosine_similarity_range = [find_best_classes_sim(similarity_classes, classes_1_range_rangeIncludes[key_1], classes_2_range_rangeIncludes[key_2]) for key_1 in keys_1 for key_2 in keys_2]
    
    flatten_cosine_similarity_labels = cosine_similarity_labels.flatten()
    flatten_cosine_similarity_comments = cosine_similarity_comments.flatten() 
    
    flatten_cosine_similarity_average = np.array([np.average([cosine_similarity_labels[i][j], cosine_similarity_comments[i][j]]) if i not in classes_1_with_no_comments and j not in classes_2_with_no_comments else cosine_similarity_labels[i][j]  for i in range(len(keys_1)) for j in range(len(keys_2))]).flatten()
    comment_used = [i not in classes_1_with_no_comments and j not in classes_2_with_no_comments  for i in range(len(keys_1)) for j in range(len(keys_2))]
    
    df = pd.DataFrame(data={
                                "Label":flatten_cosine_similarity_labels, 
                                "Comment":flatten_cosine_similarity_comments, 
                                "Average":flatten_cosine_similarity_average,
                                "Comment Used":comment_used
                            }, 
                      index=[str((i,j)) for i in keys_1 for j in keys_2])
    
    return df

def init_model():
    module_url = "https://tfhub.dev/google/universal-sentence-encoder/4"
    model = hub.load(module_url)
    return model

def embed(input):
  return model(input)


if __name__ == "__main__":
    
    config = json.load(open("config.json", "r"))

    url_server = config["URL_endpoint"]
    onto_1 = config["Ontology_1"]
    onto_2 = config["Ontology_2"]
    languages = config["Lang_Allowed"]
    sparql = SPARQLWrapper(url_server)
    sparql.setReturnFormat('json')
    sparql.method = 'GET'

    model = init_model()
    
    if type(onto_1) == str and onto_1 != "":
        classes_onto_1 = retrieve_classes_from_vocab(onto_1, languages)
        properties_onto_1 = retrieve_properties_from_vocab(onto_1, languages)
    # elif type(onto_1) == list:
    #     classes_onto_1 = retrieve_classes_from_n_vocab(onto_1, languages)
    #     properties_onto_1 = retrieve_properties_from_n_vocab(onto_1, languages)
    # else:
    #     vocabularies = retrieve_vocabularies()
    #     classes_onto_1 = retrieve_classes_from_n_vocab(vocabularies, languages)
    #     properties_onto_1 = retrieve_properties_from_n_vocab(vocabularies, languages)
    
    if type(onto_2) == str and onto_2 != "":
        classes_onto_2 = retrieve_classes_from_vocab(onto_2, languages)
        properties_onto_2 = retrieve_properties_from_vocab(onto_2, languages)
    # elif type(onto_2) == list:
    #     classes_onto_2 = retrieve_classes_from_n_vocab(onto_2, languages)
    #     properties_onto_2 = retrieve_properties_from_n_vocab(onto_2, languages)
    # else:
    #     vocabularies = retrieve_vocabularies()
    #     classes_onto_2 = retrieve_classes_from_n_vocab(vocabularies, languages)
    #     properties_onto_2 = retrieve_properties_from_n_vocab(vocabularies, languages)
    # print(classes_onto_1)
        
    a=compute_similarity_classes(classes_onto_1, classes_onto_2, "en")
    b=compute_similarity_properties(properties_onto_1, properties_onto_2, a, "en")
    print(a, b)

