package com.snaplogic.cc.spark.PipelineGraph;
import java.util.ArrayList;

/*
 * Author: Hao Chen
 * */
public abstract class GraphPipe implements java.io.Serializable {
	private ArrayList<GraphPipe> parents;
	protected String type;

	public GraphPipe(String type) throws Exception {
		this.parents = new ArrayList<GraphPipe>();
		this.type = type;
	}

	// public GraphPipe(PipeType type) throws Exception {
	// this.parents = new ArrayList<GraphPipe>();
	// this.type = type;
	// }

	public void addParent(GraphPipe pipe) {
		parents.add(pipe);
	}

	public String getType() {
		return type;
	}

	public ArrayList<GraphPipe> getParentPipe() {
		return parents;
	}
}
