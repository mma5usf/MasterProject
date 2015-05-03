package com.snaplogic.cc.spark.PipelineGraph;

/*
 * Author: Hao Chen
 * */

import java.util.List;

public class ParserPipe extends GraphPipe {
    List<String> schema;

	public ParserPipe(GraphPipe parentPipe, List<String> schema) throws Exception {
		super("parser");
		this.addParent(parentPipe);
		this.schema = schema;
	}

	public List<String> getSchema() {
		return schema;
	}
}
