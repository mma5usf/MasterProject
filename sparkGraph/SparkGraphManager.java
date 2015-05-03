package com.snaplogic.cc.spark;

import com.snaplogic.cc.spark.visitor.SparkPipelineVisitor;
import com.snaplogic.jpipe.core.graph.PipelineNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by mingluma on 4/26/15.
 */
public class SparkGraphManager {

    private static final Logger LOG = LoggerFactory.getLogger(SparkGraphManager.class);

    public void generateGraph(String ruuid, PipelineNode pipelineNode,
                                SparkPipelineVisitor sparkPipelineVisitor) {
        LOG.debug("Generate Spark Graph", ruuid);

        //visit pipelineNode
        pipelineNode.perform(sparkPipelineVisitor);
        //save spark graph to desk
        sparkPipelineVisitor.saveGraph();
    }
}
