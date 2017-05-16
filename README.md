# To replicate:

* Create spark-etl assembly jar and place in the root with the name "etl.jar" (or just modify the etl.sh script).
* run etl.sh 

Next run the following with the path to the tiles created in the previous step:

```
sbt assembly
spark-submit --driver-memory 4g target/scala-2.11/Example.jar $ABSOLUTE_PATH_TO_ETL_TILES
```

