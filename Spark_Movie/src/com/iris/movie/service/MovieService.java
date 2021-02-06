package com.iris.movie.service;

import java.io.IOException;

public interface MovieService {

	public void loadMovieData(String datasetFile,String datasetStatusFile) throws InterruptedException, IOException;
}
