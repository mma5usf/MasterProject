/*
 * SnapLogic - Data Integration
 *
 * Copyright (C) 2014, SnapLogic, Inc.  All rights reserved.
 *
 * This program is licensed under the terms of
 * the SnapLogic Commercial Subscription agreement.
 *
 * "SnapLogic" is a trademark of SnapLogic, Inc.
 */

package com.snaplogic.cc.spark.visitor;

import com.snaplogic.api.SnapException;
import com.snaplogic.cc.pipeline.visitor.ErrorTrackerUtil;
import com.snaplogic.common.jsonpath.JsonPath;
import com.snaplogic.jpipe.core.graph.Link;
import com.snaplogic.jpipe.core.graph.SnapNode;
import com.snaplogic.jpipe.core.graph.TrackingVisitor;
import com.snaplogic.jsonpath.JsonPathImpl;
import org.jboss.resteasy.spi.NotAcceptableException;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.snaplogic.cc.spark.PipelineGraph.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SparkPipelineVisitor is the visitor that creates a Simple graph for Spark job.
 *
 * Created by mingluma on 4/25/15.
 */
public class SparkPipelineVisitor extends TrackingVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(SparkPipelineVisitor.class);

    //The path we write the spark graph object to
    private static final String GRAPH_OUTPUT_PATH = "./spark_graph";

    //JsonPaths to get Snap information
    private static final JsonPath COLUMN_LIST_PATH = JsonPathImpl.compileStatic(
            "$data_map.property_map.settings.columnList.value[*].columns.value");
    private static final JsonPath CLASS_ID_PATH = JsonPathImpl.compileStatic(
            "$data_map.class_id");
    private static final JsonPath DIRECTORY_PATH = JsonPathImpl.compileStatic(
            "$data_map.property_map.settings.directory.value");
    private static final JsonPath FILE_PATH = JsonPathImpl.compileStatic(
            "$data_map.property_map.settings.file.value");

    protected final Map<String, Object> errors = new HashMap<>();

    private SparkGraph sparkGraph = new SparkGraph();

    public static class SparkGraph implements Serializable {
        private String inputPath;
        private GraphPipe sparkGraphPipe;
    }

    @Override
    protected void operateOn(final SnapNode snapNode) {
        try {
            // Endpoint checks
            Set<Link> inBoundLinks = snapNode.getInBoundLinks();
            Set<Link> outBoundLinks = snapNode.getOutboundLinks();
            Map<String, Object> snapDataMap = snapNode.getRequestData();

            if (inBoundLinks.isEmpty()) {
                // This is the source node.
                if (outBoundLinks.size() == 1) {
                    String inputDir = getInputDir(snapDataMap);
                    String inputFile = getInputFile(snapDataMap);
                    sparkGraph.inputPath = inputFile == null ?
                            inputDir : inputDir + "/" + inputFile;
                    sparkGraph.sparkGraphPipe = new SourcePipe(sparkGraph.inputPath);
                    LOG.debug("source pipe: " + sparkGraph.inputPath);
                } else {
                    errors.put(snapNode.getId(), new SnapException("SOURCE_NOT_CONNECTED_TO_ONE_SNAP"));
                }
            } else if (outBoundLinks.isEmpty()) {
                // This is the sink node.
                if (inBoundLinks.size() == 1) {
                    String outputPath = getOutputPath(snapDataMap);
                    try {
                        sparkGraph.sparkGraphPipe = new SinkPipe(sparkGraph.sparkGraphPipe, outputPath);
                        LOG.debug("sink pipe: " + outputPath);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    errors.put(snapNode.getId(),
                            new SnapException("SINK_NODE_NOT_CONNECTED_TO_ONE_SNAP"));
                }
            } else {
                // This is middle snap
                String class_id = getSnapClassID(snapDataMap);
                if (class_id.endsWith("csvparser")) {
                    //It is a parser
                    List<String> schema = getCsvColumns(snapDataMap);
                    sparkGraph.sparkGraphPipe = new ParserPipe(sparkGraph.sparkGraphPipe, schema);
                    LOG.debug("parser pipe: " + schema.toString());
                } else if (class_id.endsWith("csvformatter")) {
                    //It is a formatter
                    sparkGraph.sparkGraphPipe = new FormatterPipe(sparkGraph.sparkGraphPipe);
                    LOG.debug("formatter pipe");
                } else {
                    //It is an operator
                    String type = class_id.substring(class_id.lastIndexOf("-")+1);
                    sparkGraph.sparkGraphPipe = new OperationPipe(type, sparkGraph.sparkGraphPipe);
                    LOG.debug("operator pipe: " + type);
                }
            }
        } catch (NotAcceptableException ex) {
            ErrorTrackerUtil.handleJaxRsException(errors, snapNode.getId(), ex);
        } catch (SnapException ex) {
            ErrorTrackerUtil.handleSnapException(errors, snapNode.getId(), ex);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> getErrors() {
        return errors;
    }

    public void saveGraph() {
        try {
            ObjectOutputStream oos =
                    new ObjectOutputStream(new FileOutputStream(GRAPH_OUTPUT_PATH));
            oos.writeObject(sparkGraph);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

   // ======================================private method================================================
    private String getSnapClassID(Map<String, Object> data) {
        //Snap class id: data_map/class_id
        return CLASS_ID_PATH.readStatic(data);
    }

    private String getInputDir(Map<String, Object> data) {
        //input_file_path: /data_map/property_map/settings/directory/value
        return DIRECTORY_PATH.readStatic(data);
    }

    private String getInputFile(Map<String, Object> data) {
        //input_file_path: /data_map/property_map/settings/file/value
        return FILE_PATH.readStatic(data);
    }

    // not used
    private String getOutputPath(Map<String, Object> data) {
        // /data_map/property_map/...
        return "";
    }

    private List<String> getCsvColumns(Map<String, Object> requestData) {
        //csv parser: /data_map/property_map/settings/columnList/value[*]/columns/value
        return COLUMN_LIST_PATH.readStatic(requestData);
    }
}
