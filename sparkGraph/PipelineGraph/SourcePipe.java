package com.snaplogic.cc.spark.PipelineGraph;

/*
 * Author: Hao Chen
 * */
public class SourcePipe extends GraphPipe {
	private String filePath;

	public SourcePipe() throws Exception {
		super("source");
		this.filePath = null;
	}

	public SourcePipe(String path) throws Exception {
		super("source");
		this.filePath = path;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String path) {
		this.filePath = path;
	}
}
