package com.iris.movie.processor;

import org.apache.spark.sql.SparkSession;

import com.iris.movie.config.MovieTransformationFactory;

public abstract class BaseProcessor {

	protected SparkSession spark = MovieTransformationFactory.getSparkSession();
	public abstract void run(String...args);
}
