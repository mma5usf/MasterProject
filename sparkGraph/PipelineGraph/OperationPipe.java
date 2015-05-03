package com.snaplogic.cc.spark.PipelineGraph;
/*
 * Author: Hao Chen
 * */

import java.util.ArrayList;

public class OperationPipe extends GraphPipe {

	public OperationPipe(String type) throws Exception {
		super(type);
	}

	public OperationPipe(String type, GraphPipe parentPipe) throws Exception {
		super(type);
		this.addParent(parentPipe);
	}

	public OperationPipe(String type, ArrayList<GraphPipe> parentPipes)
			throws Exception {
		super(type);
		for (GraphPipe p : parentPipes) {
			this.addParent(p);
		}
	}
}
