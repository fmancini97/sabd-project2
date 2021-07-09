# SABD Project 2



Authors: Francesco Mancini, Fabiano Veglianti



## Requirements



- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Maven](https://maven.apache.org/)



## Building and Deployment



To build the project run
```shell
./init.sh
```



Project deployment is done using docker containers managed by docker compose.
```shell
cd ./docker-compose
docker-compose up --scale taskmanager=<number of replicas> 
```





Once all containers are running, you can submit job running
```shell
submit-job.sh [<parallelism>]
```


#### Frameworks:

- [<img src="https://www.extraordy.com/wp-content/uploads/2020/05/kafka-training-in-brighton-apache-kafka-training-1576498761.jpeg" width=70px>](http://kafka.apache.org/)


- [<img src="https://www.ververica.com/hubfs/Blog_Media/flink-logo-large-30.jpg" width=70px>](http://kafka.apache.org/)

    - Web UI port: 8081


- [<img src="https://codeblog.dotsandbrackets.com/wp-content/uploads/2017/01/graphite-logo.png" width=100px>](https://grafana.com/)


- [<img src="https://secure.gravatar.com/avatar/31cea69afa424609b2d83621b4d47f1d.jpg?s=80&r=g&d=mm" width=70px>](https://grafana.com/)
  - Web UI port: 3000

##Results

Results are stored in [*Results*](https://github.com/fmancini97/sabd-project2/tree/main/Results) folder.



- [Query 1 Weekly Result](https://github.com/fmancini97/sabd-project2/blob/main/Results/query1Weekly.csv)
- [Query 1 Monthly Result](https://github.com/fmancini97/sabd-project2/blob/main/Results/query1Monthly.csv)
- [Query 2 Weekly Result](https://github.com/fmancini97/sabd-project2/blob/main/Results/query2Weekly.csv)
- [Query 2 Monthly Result](https://github.com/fmancini97/sabd-project2/blob/main/Results/query2Monthly.csv)
- [Query 3 One Hour Result](https://github.com/fmancini97/sabd-project2/blob/main/Results/query3Hourly.csv)
- [Query 3 Two Hours Result](https://github.com/fmancini97/sabd-project2/blob/main/Results/query3EveryTwoHours.csv)