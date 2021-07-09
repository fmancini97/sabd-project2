# SABD Project 2



Authors: Francesco Mancini, Fabiano Veglianti



## Requirements



- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Maven](https://maven.apache.org/)



## Building and Deployment



To build the project run
```shell
sudo ./init.sh
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




Results are stored in [*Results*](https://github.com/fmancini97/sabd-project2/tree/main/Results/sabd/output) folder.



- [Query 1 Result](https://github.com/fmancini97/sabd-project2/blob/main/Results/sabd/output/query1Result/query1Result.csv)
- [Query 2 Result](https://github.com/fmancini97/sabd-project2/blob/main/Results/sabd/output/query2Result/query2Result.csv)
- [Query 3 Result](https://github.com/fmancini97/sabd-project2/blob/main/Results/sabd/output/query3Result/query3Result.csv)