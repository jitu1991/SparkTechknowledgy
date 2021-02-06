package com.iris.movie.model;

import java.sql.Timestamp;
import java.util.Date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MovieDFModel {
		
	Dataset<Row> datasetDf;
	
	Dataset<Row> statusDf;
	
	Dataset<Row> rejectedDf;
	
	Dataset<Row> filteredDf;

	public Dataset<Row> getDatasetDf() {
		return datasetDf;
	}

	public void setDatasetDf(Dataset<Row> datasetDf) {
		this.datasetDf = datasetDf;
	}

	public Dataset<Row> getStatusDf() {
		return statusDf;
	}

	public void setStatusDf(Dataset<Row> statusDf) {
		this.statusDf = statusDf;
	}

	public Dataset<Row> getRejectedDf() {
		return rejectedDf;
	}

	public void setRejectedDf(Dataset<Row> rejectedDf) {
		this.rejectedDf = rejectedDf;
	}

	public Dataset<Row> getFilteredDf() {
		return filteredDf;
	}

	public void setFilteredDf(Dataset<Row> filteredDf) {
		this.filteredDf = filteredDf;
	}
	

}
