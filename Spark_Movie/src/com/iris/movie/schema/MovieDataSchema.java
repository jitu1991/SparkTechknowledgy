package com.iris.movie.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MovieDataSchema {
	public static StructType schema(){
	return new StructType(new StructField[]{
    		
    		new StructField("show_id", DataTypes.StringType, true, Metadata.empty()),
    		new StructField("type", DataTypes.StringType, true, Metadata.empty()),
    		new StructField("title", DataTypes.StringType, true, Metadata.empty()),
    		new StructField("director", DataTypes.StringType, true, Metadata.empty()),
    		new StructField("cast", DataTypes.StringType, true, Metadata.empty()),
    		new StructField("country", DataTypes.StringType, true, Metadata.empty()),
    		new StructField("date_added", DataTypes.StringType, true, Metadata.empty()),
    		new StructField("release_year", DataTypes.IntegerType, true, Metadata.empty()),
    		new StructField("rating", DataTypes.StringType, true, Metadata.empty()),
    		new StructField("duration", DataTypes.StringType, true, Metadata.empty()),
    		new StructField("listed_in", DataTypes.StringType, true, Metadata.empty()),
    		new StructField("description", DataTypes.StringType, true, Metadata.empty())});
	}
}
