package com.iris.movie.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MovieStatusSchema {
	public static StructType schema(){
	return new StructType(new StructField[] {
    		new StructField("show_id", DataTypes.StringType,false, Metadata.empty()),
    		new StructField("status", DataTypes.StringType,false, Metadata.empty()),
    		
    	});

	}
}
