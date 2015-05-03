package com.snaplogic.cc.spark.PipelineGraph;

/*
 * Author: Hao Chen
 * */

public class SinkPipe extends GraphPipe {
	private String filePath;

	public SinkPipe(GraphPipe parentPipe) throws Exception {
		super("sink");
		this.filePath = null;
		this.addParent(parentPipe);
	}

	public SinkPipe(GraphPipe parentPipe, String path) throws Exception {
		super("sink");
		this.filePath = path;
		this.addParent(parentPipe);
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String path) {
		this.filePath = path;
	}
}
