echo "Args1:$1"
echo "Args2:$2"
#ROOT_PATH = "/home/team1/spark/lib"
spark-submit \
--master yarn \
--executor-memory 2g \
--driver-memory 2g \
--conf spark.dynamicAllocation.maxExecutors= 24 \
--conf spark.executor.cores = 2 \
--conf spark.lineage.enabled= false \
--conf spark.dynamicAllocation.enabled= true \
--conf spark.sql.broastcastTimeout= 1000 \
--conf spark.scheduler.mpde= FAIR \
--conf spark.eventLog.enabled= true \
--conf spark.dynamicAllocation.cachedExecutorIdleTimeout= 600 \
--conf spark.port.maxRetries= 999 \
--conf spark.driver.maxResultSize= 2G \
--conf spark.network.timeout= 1024 \
--conf spark.executor.heartBeatInterval= 60s \
--class com.iris.movie.processor.MovieProcessor /home/team1/spark/lib/Spark_Movie-0.0.1-SNAPSHOT.jar $1 $2
exist_code=$?
echo "exist_code:$exist_code"
if($exist_code -eq 0)
then
	echo "Successfully completed"
	exit 0
else
	echo "Failed during execution"
	exit 0
fi