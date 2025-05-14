# The source code for the ISWC'25 submission : _KG Nexus: Bridging the Web of Data_.

This github contains all the script used as part of the creation of the KG Nexus ressource that is currently deployed on the [website](https://kgnexus.lisn.upsaclay.fr/).


## Structure 

This github is structured to follow the sequence in which the KG Nexus was constructed. 

### Vocabulary Homogenization (LOV-RHA)

This first folder is composed of the script necessary to retrieve the informaiton from LOV dump, Wikidata and any other single ontology that an user would want to include within the KG Nexus. 

Furthermore, within these scripts we also directly apply the Harmonization to the information extracted. The output given by each of these scripts can directly be introduced within the KG Nexus and be used later on in the pipeline. 

### Retrieve Information 

In this folder, we propose scripts that will use a config file and extract all the information required by the user from KG Nexus. This data can then be used to perform the Compute of Score Alignment or the direct Alignment. 

### Compute Score Alignment 

In this folder, we propose an example of algorithm that yield scores between properties and classes from different vocabularies.

### Compute Alignment

In this folder, we propse an example of algorithm that uses the score yielded by the previous folder to decide of the alignments between vocabularies. 

### Introduce KG 

In this folder, the script that compute the signature of a KG and link the KG with the properties and classes used within the KG. 

### Introduce Keys

We introduce the scritps that will compute the keys of each KGs and connect these Keys with the classes, properties and KGs that are used within the Key.

## Example of query 

### Transfer Of Keys 
```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX kgnx: <http://kgnexus.lisn.upsaclay.fr/Nexus/>

SELECT DISTINCT ?key ?classOfKey ?kgTransferedTo
WHERE {
    ?key rdf:type kgnx:Key.
    
    ?key prov:wasDerivedFrom ?graphOfKey.
    ?graphOfKey rdf:type kgnx:KnowledgeGraph.
        
    ?key prov:used ?classOfKey.
    ?classOfKey rdf:type rdfs:Class.
    
    FILTER(?nbProp = ?nbPropTrans)
    
    {
        SELECT distinct ?key (count(distinct ?property) as ?nbProp)
        WHERE {
            ?key rdf:type kgnx:Key ;
                prov:used ?property.
        } group by ?key
    }
    
    {
        SELECT distinct ?key ?kgTransfered (count(distinct ?property) as ?nbPropTrans)
        WHERE {
            ?key rdf:type kgnx:Key ;
                prov:used ?property.
            ?property  kgnx:equivalencePropertyComputed ?propertyTransfered.
           	?kgTransferedTo prov:used ?propertyTransfered.
        } group by ?key ?kgTransfered
    }
} LIMIT 10
```
    
