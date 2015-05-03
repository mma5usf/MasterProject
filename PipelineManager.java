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

package com.snaplogic.cc.pipeline;


import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import com.snaplogic.cc.common.RecoverableError;
import com.snaplogic.cc.common.SystemManager;
import com.snaplogic.cc.guice.PlatformModule;
import com.snaplogic.cc.jstream.message.LLMessageManager;
import com.snaplogic.cc.jstream.view.feed.FeedGateway;
import com.snaplogic.cc.pipeline.visitor.ChannelRegistration;
import com.snaplogic.cc.pipeline.visitor.CloseVisitor;
import com.snaplogic.cc.pipeline.visitor.DeinitVisitor;
import com.snaplogic.cc.pipeline.visitor.InitializationVisitor;
import com.snaplogic.cc.pipeline.visitor.PrepareVisitor;
import com.snaplogic.cc.pipeline.visitor.StartVisitor;
import com.snaplogic.cc.pipeline.visitor.StatusVisitor;
import com.snaplogic.cc.pipeline.visitor.StopVisitor;
import com.snaplogic.cc.pipeline.visitor.ViewLinker;
import com.snaplogic.cc.pipeline.visitor.ViewRegistration;
import com.snaplogic.cc.pipeline.wire.PipeInfoResponse;
import com.snaplogic.cc.pipeline.wire.PipeInfoResponse.PipelineTimestamp;
import com.snaplogic.cc.pipeline.wire.PipePrepareResponse;
import com.snaplogic.cc.pipeline.wire.PipeSuggestResponse;
import com.snaplogic.cc.service.SnapManager;
import com.snaplogic.cc.service.SnapService;
import com.snaplogic.cc.sldb.SldbConnector;
import com.snaplogic.cc.snap.SnapServiceException;
import com.snaplogic.cc.snap.common.ResponseFactory;
import com.snaplogic.cc.snap.common.SerializationObjectMapperProvider;
import com.snaplogic.cc.snap.common.SnapContext;
import com.snaplogic.cc.snap.common.SnapResponse;
import com.snaplogic.cc.spark.visitor.SparkPipelineVisitor;
import com.snaplogic.cc.util.CcUtil;
import com.snaplogic.cc.util.Measurable;
import com.snaplogic.cc.yarn.HadoopGraphManager;
import com.snaplogic.cc.yarn.MRJobManager;
import com.snaplogic.cc.yarn.ParallelContextImpl;
import com.snaplogic.common.ContainerProperties;
import com.snaplogic.common.RestAPIPrefix;
import com.snaplogic.common.runtime.SnapReduceMetrics;
import com.snaplogic.common.runtime.State;
import com.snaplogic.document.lineage.MessageStore;
import com.snaplogic.jpipe.core.graph.PipelineNode;
import com.snaplogic.jpipe.core.graph.Visitor;
import com.snaplogic.snap.schema.util.JsonSchemaConstants;
import com.snaplogic.cc.spark.SparkGraphManager;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;
import javax.naming.InitialContext;
import javax.ws.rs.BadRequestException;

import cascading.flow.FlowDef;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;

import static com.snaplogic.cc.guice.PlatformModule.PIPEINFO_CACHE;
import static com.snaplogic.cc.pipeline.Messages.*;
import static com.snaplogic.common.CommonConstants.ACTIVE_PIPELINES;

/**
 * PipelineManager is the class that manages the pipeline and exposes api methods to query and
 * manipulate snaps in the pipeline. This class also adds the pipeline info response to the cache
 * if the pipeline has completed.
 *
 * @author ksubramanian
 * @since 2014
 */
public class PipelineManager {
    private static final Logger log = LoggerFactory.getLogger(PipelineManager.class);
    private static final String SEPARATOR = " and ";
    // Locks to make sure each of these api calls are made only once.
    private final Lock prepareLock = new ReentrantLock();
    private final Lock startLock = new ReentrantLock();
    private final Lock stopLock = new ReentrantLock();
    private final Lock notifyLock = new ReentrantLock();
    private final Condition notificationDone = notifyLock.newCondition();

    private final ContainerProperties containerProperties;
    private final SnapService snapService;
    private final SerializationObjectMapperProvider provider;
    private final ConcurrentMap<String, SnapManager> snapManagerStore;
    private final CcUtil ccUtil;
    private final ConcurrentMap<String, PipelineManager> activePipelineStore;
    // InfoResponse cache is populated only by PipelineManager. Consumer is PipelineResource.
    private final Cache<String, PipeInfoResponse> infoResponseCache;
    private final AtomicReference<PipePrepareResponse> pipePrepareResponse;
    private final MRJobManager mrJobManager;
    private final HadoopGraphManager hadoopGraphManager;
    private final SparkGraphManager sparkGraphManager = new SparkGraphManager();
    private final ExecutorService threadpool;
    private final SystemManager systemManager;
    private final ResponseFactory responseFactory;
    private PipelineNode pipeline;
    // Pipeline Ruuid
    private String ruuid;
    private String notificationUri;
    private Map<String, Object> llFeedContext;
    // If we need to run on YARN, we will create a flow instance during PREPARE.
    private FlowDef flowDef;
    private MRPipelineRunner mrPipelineRunner;
    private final ParallelContextImpl parallelContext = new ParallelContextImpl();
    private final FeedGateway feedGateway;
    private LLMessageManager messageManager;
    private final Map<String, SnapResponse> notificationQueue = new HashMap<>();
    private boolean notificationInProgress;
    private final Map<PipelineTimestamp, Object> timestamps = new ConcurrentHashMap<>();

    @Inject
    private Injector injector;

    @Inject
    public PipelineManager(Injector injector, ContainerProperties containerProperties,
            SnapService snapService,
            SerializationObjectMapperProvider provider, ExecutorService threadpool,
            @Named(ACTIVE_PIPELINES) ConcurrentMap<String, PipelineManager> activePipelineStore,
            @Named(PIPEINFO_CACHE) Cache<String, PipeInfoResponse> infoResponseCache,
            @Named(PlatformModule.SNAP_MANAGER)ConcurrentMap<String, SnapManager> snapManagerStore,
            CcUtil ccUtil, SystemManager systemManager,
            HadoopGraphManager hadoopGraphManager, MRJobManager mrJobManager,
            FeedGateway feedGateway, ResponseFactory responseFactory) {

        this.containerProperties = containerProperties;
        this.snapService = snapService;
        this.provider = provider;
        this.threadpool = threadpool;
        this.snapManagerStore = snapManagerStore;
        this.ccUtil = ccUtil;
        this.pipePrepareResponse = new AtomicReference<>();
        this.infoResponseCache = infoResponseCache;
        this.activePipelineStore = activePipelineStore;
        this.systemManager = systemManager;
        this.mrJobManager = mrJobManager;
        this.hadoopGraphManager = hadoopGraphManager;
        this.injector = injector;
        this.feedGateway = feedGateway;
        this.responseFactory = responseFactory;
    }

    /**
     * Returns the parent pipeline ruuid in the case of subpipelies
     *
     * @return parentRuuid
     */
    public String getParentRuuid() {
        return pipeline.getParentRuuid();
    }

    /**
     * Does a pipeline level suggest
     * @param pipeline pipeline formed from the execute data sent by slserver
     * @param notificationUri notification URI
     */
    public PipeSuggestResponse suggest(PipelineNode pipeline, String notificationUri) {
        this.pipeline = pipeline;
        this.notificationUri = notificationUri;
        preparePipeline();
        Map<String, Object> errors = hadoopGraphManager.isValid(parallelContext, pipeline);
        PipeSuggestResponse pipeSuggestResponse = new PipeSuggestResponse(provider);
        pipeSuggestResponse.addErrors(errors);
        return pipeSuggestResponse;
    }

    /**
     * Prepares the pipeline.
     *
     * @return prepareResponse
     */
    public PipePrepareResponse prepare(PipelineNode pipeline, String ruuid, String notificationUri,
                                       Map<String, Object> llFeedContext) {
        timestamps.put(PipelineTimestamp.cc_prepare_start_ms, System.currentTimeMillis());
        this.notificationUri = notificationUri;
        this.pipeline = pipeline;
        this.ruuid = ruuid;
        this.llFeedContext = llFeedContext;
        List<String> errors = this.systemManager.checkState();
        if (errors.isEmpty()) {
            this.prepareLock.lock();
            try {
                this.ruuid = ruuid;
                PipePrepareResponse prepareResponse = pipePrepareResponse.get();
                if (prepareResponse == null) {
                    // Prepare only if not prepared.
                    prepareResponse = preparePipeline();
                    pipePrepareResponse.set(prepareResponse);
                    if (prepareResponse.getErrors().isEmpty() &&
                            !RestAPIPrefix.REGULAR.equalsIgnoreCase(this.pipeline.getMode())) {
                        if (this.containerProperties.isYplexContainer()) {
                            if (hadoopGraphManager.isParallelizable(pipeline)) {
                                // Hacking, hacking, hacking, super ugly!!!
                                SparkPipelineVisitor sparkPipelineVisitor = new SparkPipelineVisitor();
                                sparkGraphManager.generateGraph(ruuid, pipeline, sparkPipelineVisitor);
                                try {
                                    flowDef = hadoopGraphManager.getFlowDefinitionFor(
                                            parallelContext, ruuid, pipeline);
                                } catch (SnapServiceException e) {
                                    prepareResponse = getErrorResponse(e.getErrors());
                                }
                            } else {
                                prepareResponse = new PipePrepareResponse(provider);
                                Map<String, Object> error = new HashMap<>(1);
                                error.put(ruuid, PIPELINE_NOT_SNAP_REDUCE_ENABLED);
                                prepareResponse.addErrors(error);
                            }
                        } else {
                            prepareResponse = new PipePrepareResponse(provider);
                            Map<String, Object> error = new HashMap<>(1);
                            error.put(ruuid, PIPELINE_CANNOT_BE_RUN_ON_REGULAR_CONTAINER);
                            prepareResponse.addErrors(error);
                        }
                    }
                }
                return prepareResponse;
            } catch (Exception e) {
                cleanupFailedPrepare();
                throw e;
            } finally {
                timestamps.put(PipelineTimestamp.cc_prepare_end_ms, System.currentTimeMillis());
                this.prepareLock.unlock();
            }
        } else {
            PipePrepareResponse prepareResponse = new PipePrepareResponse(provider);
            Map<String, Object> error = new HashMap<>(1);
            Map<String, Object> errorMessage = new HashMap<>(1);
            RecoverableError.THRESHOLD_ERROR.populate(errorMessage);
            errorMessage.put(MESSAGE, StringUtils.join(SEPARATOR, errors));
            error.put(ruuid, errorMessage);
            prepareResponse.addRecoverableErrors(errorMessage);
            return prepareResponse;
        }
    }

    /**
     * Starts all the snaps in this pipeline.
     *
     * @return snapInfoResponse list
     */
    public PipeInfoResponse start() {
        this.startLock.lock();
        try {
            StartVisitor startVisitor;
            if (preparedPipelineIsSnapReduce()) {
                log.info(RUNNING_THE_PIPELINE_AS_NATIVE_PIPELINE, ruuid);
                mrPipelineRunner = new MRPipelineRunner(parallelContext, mrJobManager, ruuid,
                        flowDef, pipeline);
                threadpool.execute(mrPipelineRunner);
                startVisitor = new StartVisitor(true);
                return startPipeline(startVisitor);
            } else {
                return startPipeline(this.ruuid, this.pipeline);
            }

        } finally {
            this.startLock.unlock();
        }
    }

    /**
     * Close the feed input views in this pipeline.
     *
     * @return snapInfoResponse list
     */
    public PipeInfoResponse close() {
        this.stopLock.lock();
        try {
            if (ruuid != null) {
                long startTime = System.currentTimeMillis();
                CloseVisitor close = new CloseVisitor();
                pipeline.perform(close);
                log.debug(TIME_TO_CLOSE_PIPELINE, ruuid, System.currentTimeMillis() - startTime);
                return cache(ruuid, close.getInfoResponses());
            } else {
                throw new BadRequestException(String.format(PIPELINE_NOT_YET_PREPARED));
            }
        } finally {
            this.stopLock.unlock();
        }
    }

    /**
     * Stops all the snaps in this pipeline.
     *
     * @return snapInfoResponse list
     */
    public PipeInfoResponse stop() {
        this.stopLock.lock();
        try {
            if (ruuid != null) {
                if (mrPipelineRunner != null) {
                    // Stop the MR Jobs running on hadoop.
                    mrPipelineRunner.stopJob();
                }
                long startTime = System.currentTimeMillis();
                StopVisitor stop = new StopVisitor();
                pipeline.perform(stop);
                log.debug(TIME_TO_STOP_PIPELINE, ruuid, System.currentTimeMillis() - startTime);
                return cache(ruuid, stop.getInfoResponses());
            } else {
                throw new BadRequestException(String.format(PIPELINE_NOT_YET_PREPARED));
            }
        } finally {
            this.stopLock.unlock();
        }
    }

    /**
     * Returns the status of all the snaps in this pipeline.
     *
     * @return snapInfoResponse list
     */
    public PipeInfoResponse info() {
        if (ruuid != null) {
            long startTime = System.currentTimeMillis();
            StatusVisitor getStatus = new StatusVisitor();
            pipeline.perform(getStatus);
            long now = System.currentTimeMillis();
            log.debug(TIME_TO_GET_PIPELINE_INFO, ruuid, now - startTime);
            List<SnapResponse> infoResponses = getStatus.getInfoResponses();
            PipeInfoResponse pipeInfoResponse = cache(ruuid, infoResponses);
            timestamps.put(PipelineTimestamp.cc_poll_ms, now);

            // Set SnapReduce Metrics (if SnapReduce Pipeline)
            if (preparedPipelineIsSnapReduce()) {
                //  Collect Pipeline Info
                FlowStats flowStats = mrPipelineRunner.getCurrentFlowStats();
                if (flowStats != null) {
                    List<FlowStepStats> stepStats = flowStats.getFlowStepStats();
                    if (stepStats != null) {
                        Map<String, Map<String, Object>> snapReduceMetricsMap =
                                new HashMap<>(stepStats.size());
                        for (FlowStepStats fss : stepStats) {
                            fss.captureDetail();
                            HadoopStepStats hss = (HadoopStepStats) fss;
                            // Sample stats map:
                            /*
                    {
                        "map_tasks": 0,
                        "duration": 3081,
                        "status": "SUCCESSFUL",
                        "name": "(1/1) ...arth/duplicate-fruits-txt",
                        "reduce_tasks": 0,
                        "reduce_progress": 0.0,
                        "map_progress": 1.0,
                        "counters": {
                            "org-apache-hadoop-mapreduce-lib-input-FileInputFormatCounter": {
                                "BYTES_READ": 0
                            },
                            "org-apache-hadoop-mapreduce-lib-output-FileOutputFormatCounter": {
                                "BYTES_WRITTEN": 71
                            },
                            "cascading-flow-SliceCounters": {
                                "Read_Duration": 1,
                                "Process_End_Time": 1425671830290,
                                "Tuples_Read": 2,
                                "Tuples_Written": 2,
                                "Write_Duration": 0,
                                "Process_Begin_Time": 1425671830287
                            },
                            "FileSystemCounters": {
                                "FILE_READ_OPS": 0,
                                "FILE_WRITE_OPS": 0,
                                "FILE_BYTES_READ": 570609080,
                                "FILE_LARGE_READ_OPS": 0,
                                "HDFS_BYTES_READ": 1552,
                                "FILE_BYTES_WRITTEN": 576136712,
                                "HDFS_LARGE_READ_OPS": 0,
                                "HDFS_BYTES_WRITTEN": 16005,
                                "HDFS_READ_OPS": 89,
                                "HDFS_WRITE_OPS": 53
                            },
                            "org-apache-hadoop-mapreduce-TaskCounter": {
                                "MAP_INPUT_RECORDS": 3,
                                "MERGED_MAP_OUTPUTS": 0,
                                "SPILLED_RECORDS": 0,
                                "COMMITTED_HEAP_BYTES": 484966400,
                                "FAILED_SHUFFLE": 0,
                                "MAP_OUTPUT_RECORDS": 2,
                                "SPLIT_RAW_BYTES": 272,
                                "GC_TIME_MILLIS": 0
                            },
                            "org-apache-hadoop-mapreduce-FileSystemCounter": {
                                "FILE_READ_OPS": 0,
                                "FILE_WRITE_OPS": 0,
                                "FILE_BYTES_READ": 570609080,
                                "FILE_LARGE_READ_OPS": 0,
                                "HDFS_BYTES_READ": 1552,
                                "FILE_BYTES_WRITTEN": 576136712,
                                "HDFS_LARGE_READ_OPS": 0,
                                "HDFS_BYTES_WRITTEN": 16005,
                                "HDFS_READ_OPS": 89,
                                "HDFS_WRITE_OPS": 53
                            },
                            "org-apache-hadoop-mapred-Task$Counter": {
                                "MAP_INPUT_RECORDS": 3,
                                "MERGED_MAP_OUTPUTS": 0,
                                "SPILLED_RECORDS": 0,
                                "COMMITTED_HEAP_BYTES": 484966400,
                                "FAILED_SHUFFLE": 0,
                                "MAP_OUTPUT_RECORDS": 2,
                                "SPLIT_RAW_BYTES": 272,
                                "GC_TIME_MILLIS": 0
                            },
                            "cascading-flow-StepCounters": {
                                "Tuples_Read": 2,
                                "Tuples_Written": 2
                            }
                        }
                    }
                             */
                            Map<String, Object> statsMap = mrPipelineRunner.getStatsFor(fss);
                            // Put other stats in the same map
                            statsMap.put(StatsConstants.MAP_PROGRESS, hss.getMapProgress());
                            statsMap.put(StatsConstants.MAP_TASKS, hss.getNumMapTasks());
                            statsMap.put(StatsConstants.REDUCE_PROGRESS, hss.getReduceProgress());
                            statsMap.put(StatsConstants.REDUCE_TASKS, hss.getNumReduceTasks());
                            snapReduceMetricsMap.put(hss.getID(), statsMap);
                        }
                        pipeInfoResponse.setSnapReduceMetrics(
                                new SnapReduceMetrics(snapReduceMetricsMap));
                    }
                }
            }
            return pipeInfoResponse;
        } else {
            throw new BadRequestException(String.format(PIPELINE_NOT_YET_PREPARED));
        }
    }

    /**
     * Recomputes the pipeline state.
     */
    public PipeInfoResponse recomputeState() {
        // Retrive the status of all the snaps in the pipeline will result in computing the
        // pipeline state.
        return info();
    }

    /**
     * Notifies the server about the snap state.
     *
     * @param snapContext
     */
    public void notify(final SnapContext snapContext) {
        enqueueNotification(snapContext.getSnapRuuid(), responseFactory
                .createInfoResponse(snapContext));

        recomputeAndNotifyParent();
    }

    /**
     * Notifies the server about the snap state.
     *
     * @param pipeInfoResponse
     */
    public void notify(final PipeInfoResponse pipeInfoResponse) {
        enqueueNotification(pipeInfoResponse.getSnapRuuid(), pipeInfoResponse);

        recomputeAndNotifyParent();
    }

    public PipeInfoResponse startPipeline(StartVisitor startVisitor) {
        if (ruuid != null) {
            long startTime = System.currentTimeMillis();
            timestamps.put(PipelineTimestamp.cc_start_ms, startTime);
            pipeline.perform(startVisitor);
            log.debug(TIME_TO_START_PIPELINE, ruuid, System.currentTimeMillis() - startTime);
            return getPipeInfoResponse(startVisitor.getInfoResponses());
        } else {
            throw new BadRequestException(String.format(PIPELINE_NOT_YET_PREPARED));
        }
    }

    public PipeInfoResponse startPipeline(String ruuid, final PipelineNode pipeline) {
        if (ruuid != null) {
            StartVisitor startVisitor = new StartVisitor();
            long startTime = System.currentTimeMillis();
            timestamps.put(PipelineTimestamp.cc_start_ms, startTime);
            pipeline.perform(startVisitor);
            log.debug(TIME_TO_START_PIPELINE, ruuid, System.currentTimeMillis() - startTime);
            return getPipeInfoResponse(startVisitor.getInfoResponses());
        } else {
            throw new BadRequestException(String.format(PIPELINE_NOT_YET_PREPARED));
        }
    }

    public InitializationVisitor initialize(String ruuid, String notificationUri,
                                            Map<String, Object> llfeedContext,
                                            final PipelineNode pipeline) {
        this.notificationUri = notificationUri;
        this.pipeline = pipeline;
        this.ruuid = ruuid;
        this.llFeedContext = llfeedContext;
        final InitializationVisitor initialization = new InitializationVisitor(injector,
                notificationUri, llfeedContext, snapService, snapManagerStore,
                activePipelineStore, messageManager);
        long initTime = ccUtil.time(new Measurable() {
            @Override
            public void run() {
                pipeline.perform(initialization);
            }
        });
        log.debug(TIME_TO_INIT_PIPELINE, ruuid, initTime);
        return initialization;
    }

    public void registerViews(String ruuid, final PipelineNode pipeline) {
        long viewRegistrationTime = ccUtil.time(new Measurable() {
            @Override
            public void run() {
                final Visitor viewRegistration = new ViewRegistration();
                pipeline.perform(viewRegistration);
            }
        });
        log.debug(TIME_TO_REGISTER_VIEW, ruuid, viewRegistrationTime);
    }

    public void linkViews(String ruuid, final PipelineNode pipeline) {
        long viewLinkTime = ccUtil.time(new Measurable() {
            @Override
            public void run() {
                final Visitor viewLinking = new ViewLinker();
                pipeline.perform(viewLinking);
            }
        });
        log.debug(TIME_TO_LINK_VIEWS, ruuid, viewLinkTime);
    }

    public void registerChannels(String ruuid, final PipelineNode pipeline) {
        long channelRegistrationTime = ccUtil.time(new Measurable() {
            @Override
            public void run() {
                final Visitor channelRegistration = new ChannelRegistration();
                pipeline.perform(channelRegistration);
            }
        });
        log.debug(TIME_TO_REGISTER_CHANNEL, ruuid, channelRegistrationTime);
    }

    public PrepareVisitor preparePipeline(String ruuid, final PipelineNode pipeline) {
        final PrepareVisitor prepareVisitor = new PrepareVisitor();
        long prepareTime = ccUtil.time(new Measurable() {
            @Override
            public void run() {
                pipeline.perform(prepareVisitor);
            }
        });
        log.debug(TIME_TO_BUILD_PREPARE_RESPONSE, ruuid, prepareTime);
        return prepareVisitor;
    }

    public PipePrepareResponse getResponseFrom(PrepareVisitor prepareVisitor) {
        final PipePrepareResponse pipePrepareResponse;
        Map<String, SnapResponse> snapPrepareResponses = prepareVisitor.getSnaps();
        pipePrepareResponse = getResponse(snapPrepareResponses);
        return pipePrepareResponse;
    }

    /**
     * Returns the prepare error response.
     *
     * @return prepareErrorResponse
     */
    public PipePrepareResponse getErrorResponse(Map<String, Object> errors) {
        PipePrepareResponse pipePrepareResponse = new PipePrepareResponse(provider);
        pipePrepareResponse.addErrors(errors);
        return pipePrepareResponse;
    }

    /**
     * Cleans up snaps when a prepare fails.
     */
    public void cleanupFailedPrepare() {
        IOUtils.closeQuietly(messageManager);
        DeinitVisitor deinitVisitor = new DeinitVisitor(snapManagerStore);
        pipeline.perform(deinitVisitor);
        timestamps.put(PipelineTimestamp.cc_end_ms, System.currentTimeMillis());
    }

    /**
     * Signal received when the JCC makes a state transition.
     *
     * @param newState The new state for the JCC.
     */
    public void handleJCCStateChange(SldbConnector.JCCState newState) {
        if (llFeedContext != null && messageManager != null) {
            if (newState != SldbConnector.JCCState.RUNNING) {
                close();
            }
        }
    }

    public void updateFeedMasters(Collection<Map<String, String>> masters) {
        if (masters.isEmpty() || llFeedContext == null || messageManager == null) {
            // Not a low-latency feed pipeline.
            return;
        }
        String feedQueueName = (String) llFeedContext.get(JsonSchemaConstants.PATH_ID);
        Collection<Map<String, String>> newMasters = messageManager.selectNewMasters(masters);
        if (newMasters.isEmpty()) {
            return;
        }
        log.debug(ADDING_NEW_FEED_MASTERS_TO_PIPELINE, newMasters);
        Map<String, InitialContext> initialContexts = feedGateway.register(newMasters,
                feedQueueName);
        messageManager.connectToFeedMasters(initialContexts);
    }

    /**
     * Add context to the current thread.
     */
    public void addThreadContext() {
        MessageStore.MESSAGE_MANAGER.set(messageManager);
    }

    //---------------------------------- Private Methods ----------------------------------------//

    private PipePrepareResponse preparePipeline() {
        PipePrepareResponse pipePrepareResponse;
        log.debug(PREPARING_PIPELINE, ruuid);
        log.debug(BUILDING_PIPELINE_GRAPH_FOR, ruuid);
        if (llFeedContext != null && !llFeedContext.isEmpty()) {
            messageManager = new LLMessageManager();
            updateFeedMasters((List<Map<String, String>>) llFeedContext.get(
                    JsonSchemaConstants.MASTERS));
        }
        log.debug(INITIALIZING_SNAPS_IN_THE_PIPELINE, ruuid);
        final InitializationVisitor initialization = initialize(ruuid, notificationUri,
                llFeedContext, pipeline);
        if (!initialization.getErrors().isEmpty()) {
            pipePrepareResponse = getErrorResponse(initialization.getErrors());
        } else {
            // No error during initialization.
            // Continue propagating views to all snaps.
            log.debug(ADDING_VIEWS, ruuid);
            registerViews(ruuid, pipeline);
            log.debug(LINKING_VIEWS_FOR, ruuid);
            linkViews(ruuid, pipeline);
            log.debug(REGISTERING_CHANNELS_FOR, ruuid);
            registerChannels(ruuid, pipeline);
            // Generate prepare response from all the snaps.
            log.debug(PREPARING_SNAPS_IN_PIPELINE, ruuid);
            final PrepareVisitor prepareVisitor = preparePipeline(ruuid, pipeline);
            if (!prepareVisitor.getErrors().isEmpty()) {
                pipePrepareResponse = getErrorResponse(initialization.getErrors());
            } else {
                pipePrepareResponse = getResponseFrom(prepareVisitor);
            }
        }
        if (!pipePrepareResponse.getErrors().isEmpty()) {
            cleanupFailedPrepare();
        }
        return pipePrepareResponse;
    }

    /**
     * This function is for readablility.  If flowDef is defined, then we had previously
     * deemed this pipeline to be SnapReduce
     * @return true is this pipeline was previously determined to be a snapreduce pipeline
     */
    private boolean preparedPipelineIsSnapReduce() {
        return flowDef != null;
    }

    private void recomputeAndNotifyParent() {
        // Compute the pipeline state and if the pipeline has a parent, notify its
        // end status to the parent pipeline
        PipeInfoResponse pipeInfo = recomputeState();
        State state = pipeInfo.getState();
        if (pipeline.getParentRuuid() != null) {
            PipelineManager parentPipelineMgr = activePipelineStore.get(pipeline.getParentRuuid());
            if ((parentPipelineMgr != null) && (State.isEnd(state))) {
                parentPipelineMgr.notify(pipeInfo);
            }
        }
    }

    /**
     * Returns the prepare response.
     *
     * @return prepareResponse
     */
    private PipePrepareResponse getResponse(
            Map<String, SnapResponse> snapPrepareResponses) {
        PipePrepareResponse pipePrepareResponse = new PipePrepareResponse(provider);
        pipePrepareResponse.addSnaps(snapPrepareResponses);
        return pipePrepareResponse;
    }

    private PipeInfoResponse cache(String runtimeUUID, List<SnapResponse> infoResponses) {
        PipeInfoResponse pipeInfoResponse = getPipeInfoResponse(infoResponses);
        State pipeState = computeStateFrom(infoResponses);
        if (State.isEnd(pipeState)) {
            infoResponseCache.put(runtimeUUID, pipeInfoResponse);
            activePipelineStore.remove(runtimeUUID, this);
            shutdownSnapManagers(pipeInfoResponse);
        }
        return pipeInfoResponse;
    }

    private State computeStateFrom(final List<SnapResponse> snapInfoResponses) {
        return State.computeFrom(Lists.transform(snapInfoResponses,
                new Function<SnapResponse, State>() {
                    @Nullable
                    @Override
                    public State apply(@Nullable final SnapResponse input) {
                        return input.getState();
                    }
                }
        ));
    }

    private PipeInfoResponse getPipeInfoResponse(List<SnapResponse> snapInfoResponses) {
        PipeInfoResponse pipeInfoResponse = new PipeInfoResponse(provider);
        pipeInfoResponse.addSnaps(snapInfoResponses);
        pipeInfoResponse.setSnapRuuid(this.ruuid);
        pipeInfoResponse.setTimestamps(timestamps);
        return pipeInfoResponse;
    }

    private void enqueueNotification(String ruuid, SnapResponse snapResponse) {
        Map<String, SnapResponse> snapsToSend = null;

        // Notifications are serialized to avoid server race condition while writing to mongo
        // store.
        notifyLock.lock();
        try {
            notificationQueue.put(ruuid, snapResponse);
            if (!notificationInProgress) {
                snapsToSend = consumeSnapNotifications();
            } else if (notificationInProgress && notificationQueue.size() == 1) {
                while (notificationInProgress) {
                    notificationDone.awaitUninterruptibly();
                }
                snapsToSend = consumeSnapNotifications();
            }
        } finally {
            notifyLock.unlock();
        }

        if (snapsToSend != null) {
            try {
                timestamps.put(PipelineTimestamp.cc_poll_ms, System.currentTimeMillis());
                ccUtil.notifyServer(notificationUri, timestamps, snapsToSend);
            } finally {
                notifyLock.lock();
                try {
                    notificationInProgress = false;
                    notificationDone.signal();
                } finally {
                    notifyLock.unlock();
                }
            }
        }
    }

    private Map<String, SnapResponse> consumeSnapNotifications() {
        Map<String, SnapResponse> retval = new HashMap<>(notificationQueue);
        notificationQueue.clear();
        notificationInProgress = true;
        return retval;
    }

    private void shutdownSnapManagers(final PipeInfoResponse pipeInfoResponse) {
        // Remove all the snap managers from the store as the pipeline has completed.
        for (SnapResponse snapInfoResponse : pipeInfoResponse.getSnapInfos()) {
            String snapRuuid = snapInfoResponse.getSnapRuuid();
            SnapManager snapManager = snapManagerStore.get(snapRuuid);
            if (snapManager != null) {
                snapManager.cleanup();
                if (snapManagerStore.remove(snapRuuid, snapManager)) {
                    log.debug(REMOVED_SNAP_MANAGER_FOR, snapRuuid);
                } else {
                    log.warn(NO_SNAP_MANAGER_FOUND_FOR, snapRuuid);
                }
            }
        }
        IOUtils.closeQuietly(messageManager);
        if (!timestamps.containsKey(PipelineTimestamp.cc_end_ms)) {
            timestamps.put(PipelineTimestamp.cc_end_ms, System.currentTimeMillis());
        }
    }
}
