package com.snaplogic.cc.spark.PipelineGraph;
/*
 * Author: Hao Chen
 * */

public class PipeType {

	public enum Type {
		SOURCE, PARSER, FORMATTER, MAPPER, FILTER, REDUCER, JOINER, SINK;
	}

	private final Type type;
	private final String typeName;

	public PipeType(String pipeType) throws Exception {
		this.typeName = pipeType;
		this.type = getType(pipeType);
	}

	public Type getType() {
		return type;
	}

	public String getTypeName() {
		return typeName;
	}

	public String toString() {
		return "Type: " + typeName;
	}

	public static Type getType(String pipeType) throws Exception {
		if (pipeType.equals("source")) {
			return Type.SOURCE;
		} else if (pipeType.equals("parser")) {
			return Type.PARSER;
		} else if (pipeType.equals("formatter")) {
			return Type.FORMATTER;
		} else if (pipeType.equals("mapper")) {
			return Type.MAPPER;
		} else if (pipeType.equals("filter")) {
			return Type.FILTER;
		} else if (pipeType.equals("sink")) {
			return Type.SINK;
		} else if (pipeType.equals("reducer")) {
			return Type.REDUCER;
		} else {
			throw new Exception(pipeType + " not defined");
		}
	}
}
