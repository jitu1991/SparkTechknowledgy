package com.iris.movie.service.impl;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.iris.movie.config.MovieTransformationFactory;
import com.iris.movie.schema.MovieDataSchema;
import com.iris.movie.schema.MovieStatusSchema;
import com.iris.movie.service.ReadMovieService;
//import org.apache.spark.sql.functions.lit;
public class ReadMovieServiceImpl implements ReadMovieService{

	private SparkSession spark = MovieTransformationFactory.getSparkSession();
	@Override
	public Dataset<Row> readMovieData(String datasetFile) {
		Dataset<Row> movieDataSet =  spark.read()
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .schema(MovieDataSchema.schema())
        .load("C:/Users/satya.mishra/Documents/Satya/MyWorkspace/Spark_Movie/src/resource/dataset_20120119_1244.csv");
		return movieDataSet.withColumn("rejected_data", lit(null));
	}

	@Override
	public Dataset<Row> readMovieStatus(String datasetStatusFile) {
		return spark.read()
		.format("com.databricks.spark.csv")
		.schema(MovieStatusSchema.schema())
		.load("C:/Users/satya.mishra/Documents/Satya/MyWorkspace/Spark_Movie/src/resource/show_dy_20210119_1244.csv");
	}

}
