import pandas as pd
from SPARQLWrapper import SPARQLWrapper, BASIC
import re
import json
from subprocess import DEVNULL, STDOUT, check_call

def curate_literal(string_to_curate:str)->str:
    return re.sub("\s", " ", string_to_curate).replace('"',' ').replace('\\',' ')

def curate_value(value:dict)->str:
    valueRelation = ""
    if value["type"] == "literal": 
        valueRelation += f'"{curate_literal(value["""value"""])}"'
        # print(valueRelation)
        if "xml:lang" in value:
            valueRelation += f'@{value["xml:lang"]}'
    else:
        valueRelation += f"<{value['value']}>"
    return valueRelation

def generate_mappings_properties(uri_ontology_start:str, uri_ontology_end:str, lang_allowed:list)-> dict:
    return {
            "IsDefinedBy":"""?property <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> ?IsDefinedBy.""",
            "Type": "?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Type.",
            "Label":"""
                ?property <http://www.w3.org/2000/01/rdf-schema#label> ?Label.
                FILTER (lang(?Label) in ( '"""+"' '".join(lang_allowed)+"""' ))
            """,
            "Comment":"""
                ?property <http://www.w3.org/2000/01/rdf-schema#comment> ?Comment.
                FILTER (lang(?Label) in ( '"""+"' '".join(lang_allowed)+"""' ))
            """,
            "Domain":"?property <http://www.w3.org/2000/01/rdf-schema#domain> ?Domain.",
            "Range":"?property <http://www.w3.org/2000/01/rdf-schema#range> ?Range.",
            "DomainIncludes":"?property <https://schema.org/domainIncludes> ?DomainIncludes.",
            "RangeIncludes":"?property <https://schema.org/rangeIncludes> ?RangeIncludes.",
            "SubPropertyOf":"""
                ?property <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?SubPropertyOf.
                # ?SubPropertyOf rdfs:isDefinedBy ?ontologies_Allowed.
                # VALUES ?ontologies_Allowed {
                #     <"""+uri_ontology_start+""">
                #     <"""+uri_ontology_end+""">
                # }
                FILTER (?property != ?SubPropertyOf)
            """,
            "EquivalentProperty":"""
                {
                    ?property <http://www.w3.org/2002/07/owl#equivalentProperty> ?EquivalentProperty.
                    ?EquivalentProperty rdfs:isDefinedBy <"""+uri_ontology_end+""">.
                } UNION {
                    ?EquivalentProperty <http://www.w3.org/2002/07/owl#equivalentProperty> ?property.
                    ?EquivalentProperty rdfs:isDefinedBy <"""+uri_ontology_end+""">.
                }
                """,
            "DifferentProperty":"""
                {
                    ?property <http://www.w3.org/2002/07/owl#differentFrom> ?EquivalentProperty.
                    ?EquivalentProperty rdfs:isDefinedBy <"""+uri_ontology_end+""">.
                } UNION {
                    ?EquivalentProperty <http://www.w3.org/2002/07/owl#differentFrom> ?property.
                    ?EquivalentProperty rdfs:isDefinedBy <"""+uri_ontology_end+""">.
                }
                """,
            "InverseOf":"""
                {
                    ?property <http://www.w3.org/2002/07/owl#inverseOf> ?InverseOf.
                    ?InverseOf rdfs:isDefinedBy <"""+uri_ontology_end+""">.
                } UNION {
                    ?InverseOf <http://www.w3.org/2002/07/owl#inverseOf> ?property.
                    ?InverseOf rdfs:isDefinedBy <"""+uri_ontology_end+""">.
                }
                """
        }

def extractPropertyFromOntology(sparql:SPARQLWrapper, uri_ontology:str, config_property:dict, mapping_information_relation:dict)-> dict:

    extracted_information = dict()

    query_init = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT DISTINCT *
        WHERE {
            ?property rdfs:isDefinedBy <"""+uri_ontology+""">.
            ?property rdf:type rdf:Property.
    """

    properties_available = set()

    query = query_init+"}"
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        properties_available.add(result["property"]["value"])

    for key in config_property:

        if config_property[key] != "False": 
            
            query = query_init+mapping_information_relation[key]+"\n}"
            seen = set()

            sparql.setQuery(query)
            response = sparql.queryAndConvert()
            for result in response["results"]["bindings"]:

                property_iri = result["property"]["value"]
                seen.add(property_iri)
                value = result[key]

                if not property_iri in extracted_information:
                    extracted_information[property_iri] = dict()
                
                if not key in extracted_information[property_iri]:
                    extracted_information[property_iri][key] = set()

                extracted_information[property_iri][key].add(curate_value(value))
            
            if config_property[key] == "True":
                properties_available.intersection_update(seen)

    return {key:extracted_information[key] for key in extracted_information if key in properties_available}

def generate_mappings_classes(uri_ontology_start:str, uri_ontology_end:str, lang_allowed:list)-> dict:
    return {
            "IsDefinedBy":"""?class <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> ?IsDefinedBy.""",
            "Type": "?class <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Type.",
            "Label":"""
                ?class <http://www.w3.org/2000/01/rdf-schema#label> ?Label.
                FILTER (lang(?Label) in ( '"""+"' '".join(lang_allowed)+"""' ))
            """,
            "Comment":"""
                ?class <http://www.w3.org/2000/01/rdf-schema#comment> ?Comment.
                FILTER (lang(?Label) in ( '"""+"' '".join(lang_allowed)+"""' ))
            """,
            "UsedInDomain":"""
                ?UsedInDomain <http://www.w3.org/2000/01/rdf-schema#domain> ?class.
                ?UsedInDomain rdf:type rdf:Property.
            """,
            "UsedInRange":"""
                ?UsedInRange <http://www.w3.org/2000/01/rdf-schema#range> ?class.
                ?UsedInRange rdf:type rdf:Property.
            """,
            "UsedInDomainIncludes":"""
                ?UsedInDomainIncludes <https://schema.org/domainIncludes> ?class.
                ?UsedInDomainIncludes rdf:type rdf:Property.
            """,
            "UsedInRangeIncludes":"""
                ?UsedInRangeIncludes <https://schema.org/rangeIncludes> ?class.
                ?UsedInRangeIncludes rdf:type rdf:Property.
            """,
            "SubClassOf":"""
                ?class <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?SubClassOf.
                # ?SubClassOf rdfs:isDefinedBy ?ontologies_Allowed.
                # VALUES ?ontologies_Allowed {
                #     <"""+uri_ontology_start+""">
                #     <"""+uri_ontology_end+""">
                # }
                # FILTER (?class != ?SubClassOf)
            """,
            "EquivalentClass":"""
                {
                    ?class <http://www.w3.org/2002/07/owl#equivalentClass> ?EquivalentClass.
                    ?EquivalentClass rdfs:isDefinedBy <"""+uri_ontology_end+""">.
                } UNION {
                    ?EquivalentClass <http://www.w3.org/2002/07/owl#equivalentClass> ?class.
                    ?EquivalentClass rdfs:isDefinedBy <"""+uri_ontology_end+""">.
                }
            """,
            "DifferentClass":"""
                {
                    ?class <http://www.w3.org/2002/07/owl#differentFrom> ?DifferentClass.
                    ?DifferentClass rdfs:isDefinedBy <"""+uri_ontology_end+""">.
                } UNION {
                    ?EquivalentProperty <http://www.w3.org/2002/07/owl#differentFrom> ?class.
                    ?DifferentClass rdfs:isDefinedBy <"""+uri_ontology_end+""">.
                }
            """
        }

def extractClassFromOntology(sparql:SPARQLWrapper, uri_ontology:str, config_class:dict, mapping_information_relation:dict) -> dict: 

    extracted_information = dict()

    query_init = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT DISTINCT *
        WHERE {
            ?class rdfs:isDefinedBy <"""+uri_ontology+""">.
            ?class rdf:type rdfs:Class.
    """

    classes_available = set()

    query = query_init+"}"
    sparql.setQuery(query)
    response = sparql.queryAndConvert()
    for result in response["results"]["bindings"]:
        classes_available.add(result["class"]["value"])

    for key in config_class:

        if config_class[key] != "False": 
            
            query = query_init+mapping_information_relation[key]+"\n}"
            seen = set()
            sparql.setQuery(query)
            response = sparql.queryAndConvert()
            for result in response["results"]["bindings"]:

                class_uri = result["class"]["value"]
                seen.add(class_uri)
                value = result[key]
                # lang = result["lang"] if "lang" in result else None
                
                # if key == "Label":
                #     print(lang)

                if not class_uri in extracted_information:
                    extracted_information[class_uri] = dict()
                
                if not key in extracted_information[class_uri]:
                    extracted_information[class_uri][key] = set()

                extracted_information[class_uri][key].add(curate_value(value))

                if key == "SubClassOf":
                    print(value)
            
            if config_class[key] == "True":
                classes_available.intersection_update(seen)

    return {key:extracted_information[key] for key in extracted_information if key in classes_available}
    
def extractOntology(sparql:SPARQLWrapper, ontology_starts:str, ontology_ends:str, config:dict) -> dict:

    data_onto = extractPropertyFromOntology(sparql, 
                                            ontology_starts, 
                                            config["Information_Property"], 
                                            generate_mappings_properties(ontology_starts, ontology_ends, config["Lang_Allowed"]))
    
    data_onto.update(extractClassFromOntology(sparql,
                                              ontology_starts, 
                                              config["Information_Class"], 
                                              generate_mappings_classes(ontology_starts, ontology_ends, config["Lang_Allowed"])))

    return data_onto

def write_ontology(data_ontology:dict, f_out):

    from_key_to_property = {
        "IsDefinedBy": ("http://www.w3.org/2000/01/rdf-schema#isDefinedBy", True),
        "Type": ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", True),
        "Label": ("http://www.w3.org/2000/01/rdf-schema#label", True),
        "Comment":("http://www.w3.org/2000/01/rdf-schema#comment", True),
        "Domain":("http://www.w3.org/2000/01/rdf-schema#domain", True),
        "Range":("http://www.w3.org/2000/01/rdf-schema#range", True),
        "DomainIncludes":("https://schema.org/domainIncludes", True),
        "RangeIncludes":("https://schema.org/rangeIncludes", True),
        "SubPropertyOf":("http://www.w3.org/2000/01/rdf-schema#subPropertyOf", True),
        "EquivalentProperty":("http://www.w3.org/2002/07/owl#equivalentProperty", True),
        "DifferentProperty":("http://www.w3.org/2002/07/owl#differentFrom", True),
        "InverseOf":("http://www.w3.org/2002/07/owl#inverseOf", True),
        "UsedInDomain":("http://www.w3.org/2000/01/rdf-schema#domain", False),
        "UsedInRange":("http://www.w3.org/2000/01/rdf-schema#range", False),
        "UsedInDomainIncludes":("https://schema.org/domainIncludes", False),
        "UsedInRangeIncludes":("https://schema.org/rangeIncludes", False),
        "SubClassOf":("http://www.w3.org/2000/01/rdf-schema#subClassOf", True),
        "EquivalentClass":("http://www.w3.org/2002/07/owl#equivalentClass", True),
        "DifferentClass":("http://www.w3.org/2002/07/owl#differentFrom", True)
    }

    for entity in data_ontology:
        for key in data_ontology[entity]:
            property, direct = from_key_to_property[key]
            for value in data_ontology[entity][key]:
                if direct:
                    f_out.write(f"<{entity}> <{property}> {value}.\n")
                else:
                    f_out.write(f"{value} <{property}> <{entity}>.\n")

if __name__ == "__main__":

    config = json.load(open("config.json", "r"))

    url_server = config["URL_endpoint"]

    onto_1 = config["Ontology_1"]
    onto_2 = config["Ontology_2"]

    sparql = SPARQLWrapper(url_server)
    sparql.setReturnFormat('json')
    sparql.method = 'GET'

    with open(f"{config['Output_Path']}{onto_1.replace('/', '_')}__TO__{onto_2.replace('/', '_')}.ttl", "w", encoding="UTF-8") as f_out:
        f_out.write(f"@prefix :  <{config['Ontology_1']}#>.\n")
        write_ontology(extractOntology(sparql, onto_1, onto_2, config), f_out)

    with open(f"{config['Output_Path']}{onto_2.replace('/', '_')}__TO__{onto_1.replace('/', '_')}.ttl", "w", encoding="UTF-8") as f_out:
        f_out.write(f"@prefix :  <{config['Ontology_2']}#>.\n")
        write_ontology(extractOntology(sparql, onto_2, onto_1, config), f_out)

    if config["Transform_Into_OWL"] == "True":
        cmd = f"java -jar rdf2rdf-1.3.0-jar-with-dependencies.jar "\
                +f"{config['Output_Path']}{onto_1.replace('/', '_')}__TO__{onto_2.replace('/', '_')}.ttl "\
                +f"{config['Output_Path']}{onto_1.replace('/', '_')}__TO__{onto_2.replace('/', '_')}.owl"
        check_call(cmd, shell=True, stdout=DEVNULL, stderr=STDOUT)

        cmd = f"java -jar rdf2rdf-1.3.0-jar-with-dependencies.jar "\
                +f"{config['Output_Path']}{onto_2.replace('/', '_')}__TO__{onto_1.replace('/', '_')}.ttl "\
                +f"{config['Output_Path']}{onto_2.replace('/', '_')}__TO__{onto_1.replace('/', '_')}.owl"
        check_call(cmd, shell=True, stdout=DEVNULL, stderr=STDOUT)
        