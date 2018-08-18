#SearchEngine

a SearchEngine implementation written in sahab internship program
.


## Used Technologies:
- Apache Kafka 
- Apache HBase
- ElasticSearch
- Apache Maven

##Used Libraries:
- Jsoup
- Jetty
- Apache Async Http Library

##How to Run:
- open configs.properties and change properties
- run your HBase, Kafka, Elastic
- run `mvn install`
- run produced jar
- `start crawler` command is for starting crawler
- `start WebServer` command is for starting WebUI
- `stop crawler` for stopping crawler
- `stop webServer` for stopping WebUI
- you can see system status at `localhost:8080/status`
- Good Lock :)