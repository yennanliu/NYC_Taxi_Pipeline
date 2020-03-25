## Elasticsearch, Logstash, Kibana (ELK) ref 

## ELK 
- ELK docker doc
	- https://elk-docker.readthedocs.io/
- ELK docker github
	- https://github.com/spujadas/elk-docker
- ELK docker hub 
	- https://hub.docker.com/r/sebp/elk/dockerfile
- ELK tutorial
	- https://www.guru99.com/elk-stack-tutorial.html

## Logstash 
- http plugin
	- https://www.elastic.co/blog/introducing-logstash-input-http-plugin
- Quick start ( Logstash -> kibana)
	- https://oranwind.org/dv-elk-an-zhuang-ji-she-ding-jiao-xue/

- grok tutorial
	- https://blog.johnwu.cc/article/elk-logstash-grok-filter.html

- Logstash input from file and grok tutorial
	- https://ithelp.ithome.com.tw/articles/10186786

### Quick start (docker)
```bash

# pull the docker image
sudo docker pull sebp/elk

# run the image directly
sudo docker run -p 5601:5601 -p 9200:9200 -p 5044:5044 -it --name elk sebp/elk
```

### Quick start (docker-compose)
```bash
git clone https://github.com/yennanliu/NYC_Taxi_Pipeline.git
cd NYC_Taxi_Pipeline/elk
docker-compose up 
```