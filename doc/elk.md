## Elasticsearch, Logstash, Kibana (ELK) ref 

## ELK 
- ELK docker doc
	- https://elk-docker.readthedocs.io/
- ELK docker github
	- https://github.com/spujadas/elk-docker
- ELK docker hub 
	- https://hub.docker.com/r/sebp/elk/dockerfile

### Quick start
```bash

# pull the docker image
sudo docker pull sebp/elk

# run the image directly
sudo docker run -p 5601:5601 -p 9200:9200 -p 5044:5044 -it --name elk sebp/elk
```