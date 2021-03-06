# Natural Language Processing Type and Annotation Browser
NLP-TAB is a web-based system designed to allow researchers and developers of Natural Language Processing (NLP) systems
to compare the output of several disparate NLP systems to each other or to a manually created reference standard.
The comparison is performed by running the NLP systems on a single corpus of text with subsequent statistical analysis
of co-occurrences between annotations generated by NLP systems. Analysis results are stored and indexed using the
ElasticSearch technology and displayed to end users with a custom web-based interface.

## System Description
### [Documents](http://athena.ahc.umn.edu/nlptab/#/document-search)
The Documents section allows for the exploration of the documents run through each of the analyzed systems. You can filter to find specific text in documents, below are a few examples on our demo server:
- [Myocardial Infarction](http://athena.ahc.umn.edu/nlptab/#/document-search?q=myocardial%20infarction)
- [Neurotonin](http://athena.ahc.umn.edu/nlptab/#/document-search?q=neurontin)
- [HF](http://athena.ahc.umn.edu/nlptab/#/document-search?q=HF)
- [6 mg q 6 hrs](http://athena.ahc.umn.edu/nlptab/#/document-search?q=8%20mg%20q%206%20hrs)

### [Type System Analysis](http://athena.ahc.umn.edu/nlptab/#/analysis)
Type system analysis performs the comparison between annotation types generated by the different NLP systems by first counting how often pairs of annotations from different NLP systems cover approximately the same text and how often they cover completely different text. This co-occurence information is used to generate 2X2 tables for all pairs of annotation types in order to calculate the degree of dependence between annotation types using common metrics which at present include the F-score, Jaccard and Matthews coefficients. Pairs of annotation types with higher scores are more likely to be functionally equivalent.

### [Type Systems](http://athena.ahc.umn.edu/nlptab/#/type-systems)
The type systems screen allows for users to explore the type systems that have been uploaded to the system, browsing the individual types in each system. Information included on the type systems page:

### Elasticsearch backend
NLP-TAB uses an Elasticsearch backend to store Common Annotation Structure (CAS) information produced by each NLP system being compared for each document in the collection. A read-only api to the backend is accessible at [athena.ahc.umn.edu/elasticsearch](http://athena.ahc.umn.edu/elasticsearch) For more information on elasticsearch, you can visit their website at [elasticsearch.org](http://www.elasticsearch.org).

## Prerequisites
1. An ElasticSearch server running version 2.1.0.
2. JDK 1.8
3. Maven.

## Building
In order to build the NLP-TAB ElasticSearch plugin run the following command in the NLP-TAB project directory.

    mvn clean package

This will build a ElasticSearch plugin zip file in target/releases, nlptab-{version}.zip. To install the plugin into
your ElasticSearch server you can type:

    bin/plugin install file:/path-to/target/releases/nlptab-{version}.zip

## About Us
NLP-TAB is developed by the
[University of Minnesota Institute for Health Informatics NLP/IE Group](http://www.bmhi.umn.edu/ihi/research/nlpie/) and
the [Open Health NLP Consortium](http://ohnlp.org/index.php/Main_Page).


## Other Resources
### BioMedICUS
*   [Demo](http://athena.ahc.umn.edu/biomedicus/)
*   [Source Code](https://github.com/nlpie/biomedicus)

### NLP-TAB
*   [Demo](http://athena.ahc.umn.edu/nlptab)
*   [Java Source Code](https://github.com/nlpie/nlptab)
*   [Web-app Source Code](https://github.com/nlpie/nlptab-webapp)
*   [Corpus](https://github.com/nlpie/nlptab-corpus)

### NLP/IE Group Resources
*   [Website](http://www.bmhi.umn.edu/ihi/research/nlpie/resources/index.htm)
*   [Demos](http://athena.ahc.umn.edu/)


## Acknowledgements
Funding for this work was provided by:
*	1 R01 LM011364-01 NIH-NLM
*	1 R01 GM102282-01A1 NIH-NIGMS
*	U54 RR026066-01A2 NIH-NCRR
