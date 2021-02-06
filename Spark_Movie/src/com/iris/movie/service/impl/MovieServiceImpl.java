package com.iris.movie.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.iris.movie.config.MovieTransformationFactory;
import com.iris.movie.dao.MovieHiveDao;
import com.iris.movie.model.MovieDFModel;
import com.iris.movie.service.MovieQueryService;
import com.iris.movie.service.MovieService;
import com.iris.movie.service.ReadMovieService;

public class MovieServiceImpl implements MovieService{
	
	private static SparkSession spark = MovieTransformationFactory.getSparkSession();
	private static ReadMovieService rms = MovieTransformationFactory.getReadMovieService();
	private static MovieQueryService mqs = MovieTransformationFactory.getMovieQueryService();
	private static MovieHiveDao mhd = MovieTransformationFactory.getMovieHiveDao();
	
	@Override
	public void loadMovieData(String datasetFile,String datasetStatusFile) throws InterruptedException, IOException {
		MovieDFModel movieDFModel = createMovieDFModel(datasetFile,datasetStatusFile);
        validateMovieType(movieDFModel);
        Dataset<Row> validYearDf = validateReleaseYear(movieDFModel);
        Dataset<Row> numericShowIdDf = validateShowIdNumeric(movieDFModel,validYearDf);
        Dataset<Row> filteredDataDf = validateShowIdStatus(movieDFModel, numericShowIdDf);
        movieDFModel.setFilteredDf(filteredDataDf);
        writeToHive(movieDFModel);
        mhd.writeToHistory(movieDFModel);
        mqs.solveQuery();
	}
	
	private void writeToHive(MovieDFModel movieDFModel) {
		mhd.writeToHive(movieDFModel.getFilteredDf());
	}
	private MovieDFModel createMovieDFModel(String datasetFile,String datasetStatusFile){
		MovieDFModel mDFM = new MovieDFModel();
		mDFM.setDatasetDf(rms.readMovieData(datasetFile));
		mDFM.setStatusDf(rms.readMovieStatus(datasetStatusFile));
		return mDFM;
	}
	private void validateMovieType(MovieDFModel movieDFModel) {
		Dataset<Row> datasetDf = movieDFModel.getDatasetDf();
		Dataset<Row> rejectedDf = movieDFModel.getRejectedDf();
		datasetDf.createOrReplaceTempView("movie_raw_feed_s");

		Dataset<Row> correctTypeDf = spark
				.sql("select * from movie_raw_feed_s where type = 'Movie' or type = 'TV Show' ");

		Dataset<Row> invalidTypeDf = spark.createDataFrame(spark
				.sql("select * from movie_raw_feed_s where type <> 'Movie' and type <> 'TV Show' ").javaRDD().map(row -> {
			return RowFactory.create(row.getAs("show_id"), row.getAs("type"), row.getAs("title"), row.getAs("director"),
					row.getAs("cast"), row.getAs("country"), row.getAs("date_added"), row.getAs("release_year"),
					row.getAs("rating"), row.getAs("duration"), row.getAs("listed_in"), row.getAs("description"));
		}), datasetDf.schema()).withColumn("rejected_data", functions.lit("Invalid Type !"));
		
		rejectedDf = invalidTypeDf;
		movieDFModel.setRejectedDf(rejectedDf);
		movieDFModel.setFilteredDf(correctTypeDf);
	}
	
	private static Dataset<Row> validateReleaseYear(MovieDFModel movieDFModel){
		Dataset<Row> correctTypeDf = movieDFModel.getFilteredDf();
		correctTypeDf.createOrReplaceTempView("movie_raw_feed_s");
		
		Dataset<Row> numericReleaseYearDf = spark.sql("select * from movie_raw_feed_s where (release_year BETWEEN 1800 AND 9999) and (release_year REGEXP '^[0-9]+$') ");
		
		Dataset<Row> nonNumericReleaseYearDf = correctTypeDf.union(numericReleaseYearDf).except(correctTypeDf.intersect(numericReleaseYearDf)); 
		
		Dataset<Row> invalidReleaseYearDf = spark.createDataFrame(nonNumericReleaseYearDf.javaRDD().map(row -> {
			return RowFactory.create(row.getAs("show_id"), row.getAs("type"), row.getAs("title"), row.getAs("director"),
					row.getAs("cast"), row.getAs("country"), row.getAs("date_added"), row.getAs("release_year"),
					row.getAs("rating"), row.getAs("duration"), row.getAs("listed_in"), row.getAs("description"));
		}), movieDFModel.getDatasetDf().schema()).withColumn("rejected_data", functions.lit("Invalid Release year !"));
		
		movieDFModel.getRejectedDf().union(invalidReleaseYearDf);

		return numericReleaseYearDf;
	}
	
	
	private static Dataset<Row> validateShowIdNumeric(MovieDFModel movieDFModel, Dataset<Row> validYearDf){
	
		validYearDf.createOrReplaceTempView("movie_raw_feed_s");
		Dataset<Row> numericShowIdDf = spark.sql("select * from movie_raw_feed_s where show_id REGEXP '^[0-9]+$' ");
		
		
		Dataset<Row> invalidShowIdDf = spark.createDataFrame(validYearDf.except(numericShowIdDf).javaRDD().map(row -> {
			return RowFactory.create(row.getAs("show_id"), row.getAs("type"), row.getAs("title"), row.getAs("director"),
					row.getAs("cast"), row.getAs("country"), row.getAs("date_added"), row.getAs("release_year"),
					row.getAs("rating"), row.getAs("duration"), row.getAs("listed_in"), row.getAs("description"));
		}), movieDFModel.getDatasetDf().schema()).withColumn("rejected_data", functions.lit("Non-numeric Show ID !"));
		
		movieDFModel.getRejectedDf().union(invalidShowIdDf);
		
		return numericShowIdDf;
	}
	
	private static Dataset<Row> validateShowIdStatus(MovieDFModel movieDFModel, Dataset<Row> numericShowIdDf) {
		Dataset<Row> statusDf = movieDFModel.getStatusDf();
		Dataset <Row> joinedData = numericShowIdDf.join(statusDf, numericShowIdDf.col("show_id").equalTo(statusDf.col("show_id"))).drop(statusDf.col("show_id"));
		joinedData.createOrReplaceTempView("movie_raw_feed_s");
		//joinedData.show();
		Dataset<Row> filteredDataDf = spark.sql("select * from movie_raw_feed_s where status = 'A' ");
		filteredDataDf.show();
		Dataset<Row> inactiveShowIdDf = spark.createDataFrame(spark.sql("select * from movie_raw_feed_s where status = 'I' ").javaRDD().map(row -> {
			return RowFactory.create(row.getAs("show_id"), row.getAs("type"), row.getAs("title"), row.getAs("director"),
					row.getAs("cast"), row.getAs("country"), row.getAs("date_added"), row.getAs("release_year"),
					row.getAs("rating"), row.getAs("duration"), row.getAs("listed_in"), row.getAs("description"),row.getAs("status"));
		}), movieDFModel.getDatasetDf().schema()).withColumn("rejected_data", functions.lit(" Show ID is inactive !"));
		inactiveShowIdDf.show();
		//filteredDataDf.drop("status");
		movieDFModel.getRejectedDf().union(inactiveShowIdDf);
		//movieDFModel.getRejectedDf().show();
		return filteredDataDf;
	}

}
