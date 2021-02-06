package com.iris.movie.dao.impl;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.time.LocalDate;
import java.util.Date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.iris.movie.config.MovieTransformationFactory;
import com.iris.movie.dao.MovieHiveDao;
import com.iris.movie.model.MovieDFModel;

public class MovieHiveDaoImpl implements MovieHiveDao{
	
	private final static String MOVIE_DATA_TEMP_VIEW = "MOVIE_DATA_TEMP_VIEW";
	private final static String HIVE_ACTIVE_TABLE_NAME = "IRIS_MOVIE_DATA";
	private final static String HIVE_HISTORY_TABLE_NAME = "IRIS_MOVIE_DATA_HISTORY";
	private final static String HIVE_TABLE_COLUMN_NAME = "show_id,type,title,director,star_cast,country,date_added,release_year,rating,duration,listed_in,description,status";
	private static SparkSession spark = MovieTransformationFactory.getSparkSession();

	@Override
	public void writeToHive(Dataset<Row> activeDF) {
		activeDF=activeDF.withColumnRenamed("cast","star_cast");
		activeDF=activeDF.withColumn("created_date",lit(LocalDate.now()));
		activeDF.drop("rejected_data");
		activeDF.createOrReplaceTempView(MOVIE_DATA_TEMP_VIEW);
		StringBuilder query = new StringBuilder("INSERT ").append("OVERWRITE ").append(" TABLE ").append(HIVE_ACTIVE_TABLE_NAME)
				.append(" PARTITION (show_id, created_date)").append("SELECT ").append(HIVE_TABLE_COLUMN_NAME).append(" FROM ").append(MOVIE_DATA_TEMP_VIEW);
		spark.sql(query.toString());
	}

	@Override
	public void writeToHistory(MovieDFModel movieDFModel) {
		StringBuilder query = new StringBuilder("select * from ").append(HIVE_ACTIVE_TABLE_NAME).append(" where Created_Date >= DATEADD(day,-7, GETDATE())");
		Dataset<Row> activeDF = spark.sql(query.toString());
		Dataset<Row> historyDF = movieDFModel.getDatasetDf().filter(col("show_id").notEqual(activeDF.col("show_id")));
		historyDF=historyDF.withColumnRenamed("cast","star_cast");
		historyDF=historyDF.withColumn("created_date",lit(LocalDate.now()));
		historyDF.drop("rejected_data");
		historyDF.createOrReplaceTempView(MOVIE_DATA_TEMP_VIEW);
		StringBuilder writeQuery = new StringBuilder("INSERT ").append("OVERWRITE ").append(" TABLE ").append(HIVE_HISTORY_TABLE_NAME)
				.append(" PARTITION (show_id, created_date)").append("SELECT ").append(HIVE_TABLE_COLUMN_NAME).append(" FROM ").append(MOVIE_DATA_TEMP_VIEW);
		spark.sql(writeQuery.toString());
		writeToHive(activeDF);
	}

}
