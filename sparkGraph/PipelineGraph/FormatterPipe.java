package com.snaplogic.cc.spark.PipelineGraph;

/*
 * Author: Hao Chen
 * */

public class FormatterPipe extends GraphPipe {

	public FormatterPipe(GraphPipe parentPipe) throws Exception {
		super("formatter");
		this.addParent(parentPipe);
	}
}
