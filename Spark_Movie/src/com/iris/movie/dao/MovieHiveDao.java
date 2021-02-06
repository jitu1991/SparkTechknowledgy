package com.iris.movie.dao;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.iris.movie.model.MovieDFModel;

public interface MovieHiveDao {

	public void writeToHive(Dataset<Row> activeDF);
	public void writeToHistory(MovieDFModel movieDFModel);
}
