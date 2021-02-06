package com.iris.movie.service.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import com.iris.movie.config.MovieTransformationFactory;
import com.iris.movie.service.MovieQueryService;

public class MovieQueryServiceImpl implements MovieQueryService{

	private static SparkSession spark = MovieTransformationFactory.getSparkSession();
	@Override
	public void solveQuery() {
		Dataset<Row> dataset = spark.sql("select * from IRIS_MOVIE_DATA where status='A'");	
		
		System.out.println("--------------- Type of Content Available ---------------");
    	showTypeOfContentAvailable(dataset);
    	
    	System.out.println("--------------- Growth of content over years ---------------");
    	Dataset<Row> datasetWithTitleAndDate = getDatasetOfTitleWithTimestamp(dataset);
    	showContentGrowthOverYears(datasetWithTitleAndDate);
    	
    	System.out.println("--------------- Month when content added the most - Top 20 ---------------");
    	showContentAddedMostByMonth(datasetWithTitleAndDate);
    	
    	System.out.println("--------------- 20 Oldest content ---------------");
    	show20OldestContent(datasetWithTitleAndDate);
    	
    	System.out.println("--------------- Country wise content ---------------");
    	showContentByCountry(dataset);
    	
    	System.out.println("--------------- Country wise and content wise ---------------");
    	showByCountryAndContent(dataset);
    	
    	System.out.println("--------------- Content Rating ---------------");
    	showRatingOfContent(dataset);

    	Dataset<Row> datasetByCategeries = dataset.withColumn("listed_in", explode(split(col("listed_in"), "\\, ")));
    	System.out.println("--------------- Top Categories ---------------");
    	showContentByTopCategeries(datasetByCategeries);

    	System.out.println("--------------- Stand-up comedies by Country with release year ---------------");
    	showStandUpComediesWithReleaseYearCountryWise(datasetByCategeries);

    	Dataset<Row> dataListedByActors = dataset.withColumn("cast", explode(split(col("cast"), "\\, ")));
    	System.out.println("--------------- Top actors with most movies ---------------");
    	showTopActorsWithMostMovies(dataListedByActors);

    	System.out.println("--------------- Top actors with most tv shows ---------------");
    	showTopActorsWithMostTvShows(dataListedByActors);

    	Dataset<Row> datasetByMovieDirectors = dataset.filter(col("type").equalTo("Movie")).withColumn("director", explode(split(col("director"), "\\, ")));
    	System.out.println("--------------- Top directors with most content ---------------");
    	showMovieDirectorsWithMostContent(datasetByMovieDirectors);

    	System.out.println("--------------- Top movie directors from India ---------------");
    	showTopMovieDirectorFromIndia(datasetByMovieDirectors);

    	System.out.println("--------------- TV Show seasons ---------------");
    	showTvShowsWithSeasonCount(dataset);
    	//session.clearActiveSession();
	}
	
	private static void showTvShowsWithSeasonCount(Dataset<Row> dataset) {
		dataset.select(col("title"),col("duration")).filter(col("type").equalTo("TV Show").and(col("duration").contains("Season"))).groupBy(col("title")).count().sort("count").orderBy(desc("count")).withColumnRenamed("count", "Seasons").limit(20).show(false);
	}

	private static void showTopMovieDirectorFromIndia(Dataset<Row> datasetByDirectors) {
		datasetByDirectors.filter(col("director").notEqual("").and(col("country").contains("India"))).groupBy("director").count().sort("count").orderBy(desc("count")).show(false);
	}

	private static void showMovieDirectorsWithMostContent(Dataset<Row> dataListedByDirectors) {
		dataListedByDirectors.filter(col("director").notEqual("")).groupBy("director").count().sort("count").orderBy(desc("count")).limit(20).show(false);
	}

	private static void showTopActorsWithMostTvShows(Dataset<Row> dataListedByActors) {
		dataListedByActors.select(col("cast"),col("country")).filter(col("cast").notEqual("").and(col("type").contains("TV Show"))).groupBy(col("cast")).count().sort("count").orderBy(desc("count")).limit(20).show();
	}

	private static void showTopActorsWithMostMovies(Dataset<Row> dataListedByActors) {
		dataListedByActors.select(col("cast"),col("country")).filter(col("cast").notEqual("").and(col("type").contains("Movie"))).groupBy(col("cast")).count().sort("count").orderBy(desc("count")).limit(20).show(false);
	}

	private static void showStandUpComediesWithReleaseYearCountryWise(Dataset<Row> datasetByCategeries) {
		datasetByCategeries.select(col("title"),col("country"),col("release_year")).filter(col("listed_in").contains("Stand-Up Comedy").and(col("country").notEqual(""))).sort(col("country")).show(false);
	}

	private static void showContentByTopCategeries(Dataset<Row> datasetByCategeries) {
		datasetByCategeries.groupBy(col("listed_in")).count().sort("count").orderBy(desc("count")).show(false);
	}

	private static void showRatingOfContent(Dataset<Row> dataset) {
		dataset.select("title","rating").show(false);
	}

	private static void showByCountryAndContent(Dataset<Row> dataset) {
		dataset.select(col("country"), col("title")).filter(col("country").notEqual("")).sort(col("country"), col("title")).show(false);
	}

	private static void showContentByCountry(Dataset<Row> dataset) {
		dataset.select(col("country"), col("title")).filter(col("country").notEqual("")).sort(col("country")).show(false);
	}

	private static void show20OldestContent(Dataset<Row> datasetWithTitleAndDate) {
		datasetWithTitleAndDate.orderBy(col("date_added")).limit(20).show(false);
	}

	private static void showContentAddedMostByMonth(Dataset<Row> datasetWithTitleAndDate) {
		Row highestContentInMonth = datasetWithTitleAndDate.select(month(col("date_added")).as("Month")).groupBy("Month").count().sort("count").orderBy(desc("count")).first();
		Object month = highestContentInMonth.getAs("Month");
		System.out.println(month.toString());
		datasetWithTitleAndDate.select(col("title"), col("date_added"),month(col("date_added")).as("Month")).filter(col("Month").equalTo(month)).orderBy(col("date_added")).show(false);
	}

	private static void showContentGrowthOverYears(Dataset<Row> datasetWithTitleAndDate) {
		datasetWithTitleAndDate.select(year(col("date_added")).as("Year")).groupBy("Year").count().sort("Year").show(false);
	}

	private static Dataset<Row> getDatasetOfTitleWithTimestamp(Dataset<Row> dataset) {
		return dataset.select(col("title"), col("date_added")).withColumn("date_added", unix_timestamp(col("date_added"), "MMMM dd,yyyy").cast("timestamp"));
	}

	private static void showTypeOfContentAvailable(Dataset<Row> dataset) {
		dataset.select(col("type")).distinct().show(false);
	}

}
