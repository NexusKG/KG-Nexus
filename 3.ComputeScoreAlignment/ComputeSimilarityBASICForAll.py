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
from tqdm import tqdm

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
        SELECT ?uri_property ?label ?comment
        WHERE {
            ?uri_property rdfs:isDefinedBy <"""+uri_vocab+""">;
                    rdf:type rdf:Property;
                    rdfs:label ?label.
            FILTER(lang(?label) in ('"""+"', '".join(languages)+"""'))
            OPTIONAL{
                ?uri_property rdfs:comment ?comment.
                FILTER(lang(?comment) = lang(?label))
            }
        }
        """
    
    properties = dict()
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:

        uri = result["uri_property"]["value"]
        label = result["label"]
        comment = None
        if "comment" in result:
            comment = result["comment"]

        if uri not in properties :
            properties[uri] = {
                "label":dict(),
                "comment":dict()
            }

        properties[uri]["label"][label["xml:lang"]] = label["value"]

        if comment:
            properties[uri]["comment"][comment["xml:lang"]] = comment["value"]
    
    return properties

def compute_similarity(data_1, data_2, language="en"):
    if len(data_1.keys())>0 and len(data_2.keys())>0:
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
                                    "Components" : [(i,j) for i in keys_1 for j in keys_2],
                                    "Label":flatten_cosine_similarity_labels, 
                                    "Comment":flatten_cosine_similarity_comments, 
                                    "Average":flatten_cosine_similarity_average,
                                    "Comment Used":comment_used
                                }, 
                        index=[str((i,j)) for i in keys_1 for j in keys_2])
    else:
        df = pd.DataFrame(data=[], columns=["Components", "Label", "Comment", "Average", "Comment Used"])

    return df

def init_model():
    module_url = "https://tfhub.dev/google/universal-sentence-encoder/4"
    model = hub.load(module_url)
    return model

def embed(input):
  return model(input)

def from_value_to_interval(value):
    for i in np.arange(global_threshold, 1.5, intervals):
        if value < i:
            return f"{np.around(i-intervals, precision)}_{np.around(i, precision)}" 
    return None

if __name__ == "__main__":
    print('Start')
    config = json.load(open("config.json", "r"))

    url_server = config["URL_endpoint"]
    languages = config["Lang_Allowed"]
    sparql = SPARQLWrapper(url_server)
    sparql.setReturnFormat('json')
    sparql.method = 'GET'
    print('2')
    global_threshold = 0.5
    intervals = 0.01
    precision = 2

    model = init_model()
    print('Load Data')    
    
    vocabularies = list(retrieve_vocabularies())
    classes_per_onto = retrieve_classes_from_n_vocab(vocabularies, languages)
    properties_per_onto = retrieve_properties_from_n_vocab(vocabularies, languages)
    print('Compute Similarity')
    with open("./Similarity.nq", "w", encoding="UTF-8") as f_out:
        f = open("./follow", "w")
        for i in tqdm((range(10)), file=f): #tqdm(range(len(vocabularies)), file=f):
            for j in range(i+1, 10):
                # sim_class = pd.concat([sim_class, compute_similarity(classes_per_onto[vocabularies[i]], classes_per_onto[vocabularies[j]], "en")])
                # sim_props = pd.concat([sim_props, compute_similarity(properties_per_onto[vocabularies[i]], classes_per_onto[vocabularies[j]], "en")])
                df_c = (compute_similarity(classes_per_onto[vocabularies[i]], classes_per_onto[vocabularies[j]], "en"))
                for (c1, c2), average in df_c[["Components", "Average"]].values:
                    if average > global_threshold:
                        f_out.write(f"<{c1}> <http://KG_nexux.com/equivalenceScore> <{c2}> <http://value/{from_value_to_interval(average)}>.\n")
                
                df_p = (compute_similarity(properties_per_onto[vocabularies[i]], properties_per_onto[vocabularies[j]], "en"))
                for (c1, c2), average in df_p[["Components", "Average"]].values:
                    if average > global_threshold:
                        f_out.write(f"<{c1}> <http://KG_nexux.com/equivalenceScore> <{c2}> <http://value/{from_value_to_interval(average)}>.\n")
        
        f.close()




