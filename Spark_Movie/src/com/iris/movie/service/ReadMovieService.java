package com.iris.movie.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface ReadMovieService {

	public Dataset<Row> readMovieData(String datasetFile);
	public Dataset<Row> readMovieStatus(String datasetStatusFile);
}
