package com.iris.movie.processor;


import java.io.IOException;

import com.iris.movie.config.MovieTransformationFactory;
import com.iris.movie.service.MovieService;

public class MovieProcessor extends BaseProcessor{


   private MovieService ms = MovieTransformationFactory.getMovieService();
   @Override
    public void run(String[] args) {

    	try {
        	//System.setProperty("hadoop.home.dir", "c:\\winutil\\");
        	String datasetFile = "";//"C:/Users/satya.mishra/Documents/Satya/MyWorkspace/Spark_Movie/src/resource/dataset_20120119_1244.csv";
        	String datasetStatusFile = "";//"C:/Users/satya.mishra/Documents/Satya/MyWorkspace/Spark_Movie/src/resource/show_dy_20210119_1244.csv";
        	if(args.length>0){
        		datasetFile = args[0];
            	datasetStatusFile = args[1];
        	}
        	
        	ms.loadMovieData(datasetFile,datasetStatusFile);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }

	
	public static void main(String... args) {
		MovieTransformationFactory.run(MovieProcessor.class, args);
	}
}
