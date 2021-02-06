package com.iris.movie.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SparkSession;

import com.iris.movie.dao.MovieHiveDao;
import com.iris.movie.dao.impl.MovieHiveDaoImpl;
import com.iris.movie.processor.BaseProcessor;
import com.iris.movie.service.MovieQueryService;
import com.iris.movie.service.MovieService;
import com.iris.movie.service.ReadMovieService;
import com.iris.movie.service.impl.MovieQueryServiceImpl;
import com.iris.movie.service.impl.MovieServiceImpl;
import com.iris.movie.service.impl.ReadMovieServiceImpl;

public class MovieTransformationFactory {

	private static final Map<Class<?>,Object> INSTANCES = new HashMap();
	private static SparkSession spark;
	
	private MovieTransformationFactory(){
		
	}
	@SuppressWarnings({"unchecked" })
	private static <T> T getOrCreateInstance(Class<T> classz){
		try {
			if(!INSTANCES.containsKey(classz)){
				INSTANCES.put(classz, classz.newInstance());
			}
			return (T) INSTANCES.get(classz);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}
	public static SparkSession getSparkSession(){
		if(spark==null){
			spark = SparkSession.builder().appName("SparkMovie").enableHiveSupport()
			.config("hive.exec.dynamic.partion", "true")
			//.config("spark.sql.warehouse.dir", "file:///c:/temp/")
			.config("hive.exec.dynamic.partion.mode", "nonstrict")
			.config("spark.master", "local[*]")
            .getOrCreate();
		}
		return spark;
	}
	
	public static MovieService getMovieService(){
		return getOrCreateInstance(MovieServiceImpl.class);
	}
	
	public static ReadMovieService getReadMovieService(){
		return getOrCreateInstance(ReadMovieServiceImpl.class);
	}
	
	public static MovieQueryService getMovieQueryService(){
		return getOrCreateInstance(MovieQueryServiceImpl.class);
	}
	
	public static MovieHiveDao getMovieHiveDao(){
		return getOrCreateInstance(MovieHiveDaoImpl.class);
	}
	
	public static void run(Class<?> mainClass, String[] args){
		try {
			Object obj = mainClass.newInstance();
			if(obj instanceof BaseProcessor){
				((BaseProcessor) obj).run(args);
			}
		} catch (InstantiationException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
