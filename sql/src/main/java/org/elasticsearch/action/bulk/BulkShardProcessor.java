/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.elasticsearch.action.bulk;

import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.core.collections.Row;
import io.crate.exceptions.Exceptions;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.ShardUpsertResponse;
import io.crate.operation.collect.ShardingProjector;
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * Processor to do Bulk Inserts, similar to {@link BulkProcessor}
 * but less flexible (only supports IndexRequests)
 *
 * If the Bulk threadPool Queue is full retries are made and
 * the {@link #add} method will start to block.
 */
public class BulkShardProcessor {

    private static final AssignmentVisitor assignmentVisitor = new AssignmentVisitor();

    private final ClusterService clusterService;
    private final TransportShardUpsertActionDelegate transportShardUpsertActionDelegate;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final boolean autoCreateIndices;
    private final int bulkSize;
    private final Map<ShardId, ShardUpsertRequest> requestsByShard = new HashMap<>();
    private final AutoCreateIndex autoCreateIndex;
    private final AtomicInteger globalCounter = new AtomicInteger(0);
    private final AtomicInteger counter = new AtomicInteger(0);
    private final SettableFuture<BitSet> result;
    private final AtomicInteger pending = new AtomicInteger(0);
    private final AtomicInteger currentDelay = new AtomicInteger(0);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final BitSet responses;
    private final Object responsesLock = new Object();
    private final boolean overwriteDuplicates;
    private volatile boolean closed = false;
    private final Set<String> indicesCreated = new HashSet<>();
    private final ReadWriteLock retryLock = new ReadWriteLock();
    private final Semaphore executeLock = new Semaphore(1);
    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(daemonThreadFactory("bulkShardProcessor"));
    private final TimeValue requestTimeout;
    private final boolean continueOnErrors;
    private final ShardingProjector shardingProjector;
    private final AssignmentVisitorContext assignmentVisitorContext;

    @Nullable
    private Map<Reference, Symbol> updateAssignments;
    @Nullable
    private Map<Reference, Symbol> insertAssignments;


    private final ESLogger logger = Loggers.getLogger(getClass());

    public BulkShardProcessor(ClusterService clusterService,
                              Settings settings,
                              TransportShardUpsertActionDelegate transportShardUpsertActionDelegate,
                              TransportCreateIndexAction transportCreateIndexAction,
                              ShardingProjector shardingProjector,
                              boolean autoCreateIndices,
                              boolean overwriteDuplicates,
                              int bulkSize,
                              boolean continueOnErrors,
                              @Nullable Map<Reference, Symbol> updateAssignments,
                              @Nullable Map<Reference, Symbol> insertAssignments) {
        assert updateAssignments != null | insertAssignments != null;
        this.clusterService = clusterService;
        this.transportShardUpsertActionDelegate = transportShardUpsertActionDelegate;
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.shardingProjector = shardingProjector;
        this.autoCreateIndices = autoCreateIndices;
        this.bulkSize = bulkSize;
        this.continueOnErrors = continueOnErrors;
        this.overwriteDuplicates = overwriteDuplicates;
        this.updateAssignments = updateAssignments;
        this.insertAssignments = insertAssignments;
        responses = new BitSet();
        result = SettableFuture.create();
        autoCreateIndex = new AutoCreateIndex(settings);
        requestTimeout = settings.getAsTime("insert_by_query.request_timeout", BulkShardRequest.DEFAULT_TIMEOUT);
        assignmentVisitorContext = processAssignments(updateAssignments, insertAssignments);
    }

    public boolean add(String indexName,
                       Row row,
                       @Nullable Long version) {
        pending.incrementAndGet();
        Throwable throwable = failure.get();
        if (throwable != null) {
            result.setException(throwable);
            return false;
        }

        if (autoCreateIndices) {
            createIndexIfRequired(indexName);
        }

        // will only block if retries/writer are active
        try {
            retryLock.readLock();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }

        shardingProjector.setNextRow(row);

        partitionRequestByShard(indexName, shardingProjector.id(), row, shardingProjector.routing(), version);
        executeIfNeeded();
        return true;
    }

    private void partitionRequestByShard(String indexName,
                                         String id,
                                         Row row,
                                         @Nullable String routing,
                                         @Nullable Long version) {
        ShardId shardId = clusterService.operationRouting().indexShards(
                clusterService.state(),
                indexName,
                Constants.DEFAULT_MAPPING_TYPE,
                id,
                routing
        ).shardId();


        try {
            executeLock.acquire();
            ShardUpsertRequest updateRequest = requestsByShard.get(shardId);
            if (updateRequest == null) {
                updateRequest = new ShardUpsertRequest(
                        shardId,
                        assignmentVisitorContext.dataTypes(),
                        assignmentVisitorContext.columnIndicesToStream(),
                        updateAssignments,
                        insertAssignments);
                updateRequest.timeout(requestTimeout);
                updateRequest.continueOnError(continueOnErrors);
                updateRequest.overwriteDuplicates(overwriteDuplicates);
                requestsByShard.put(shardId, updateRequest);
            }
            counter.getAndIncrement();
            updateRequest.add(globalCounter.getAndIncrement(), id, row, version, routing);
        } catch (InterruptedException e) {
            Thread.interrupted();
        } finally {
            executeLock.release();
        }
    }

    public ListenableFuture<BitSet> result() {
        return result;
    }

    public void close() {
        trace("close");
        closed = true;
        executeRequests();
        if (pending.get() == 0) {
            setResult();
        }
        stopExecutor();
    }

    private void stopExecutor() {
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            scheduledExecutorService.shutdownNow();
        }
    }

    private void setFailure(Throwable e) {
        failure.compareAndSet(null, e);
        result.setException(e);
        stopExecutor();
    }

    private void setResult() {
        Throwable throwable = failure.get();
        if (throwable == null) {
            result.set(responses);
        } else {
            result.setException(throwable);
        }
    }

    private void setResultIfDone(int successes) {
        for (int i = 0; i < successes; i++) {
            if (pending.decrementAndGet() == 0 && closed) {
                setResult();
            }
        }
    }

    private void executeIfNeeded() {
        if (closed || counter.get() >= bulkSize) {
            executeRequests();
        }
    }

    private void executeRequests() {
        try {
            executeLock.acquire();
            for (Iterator<Map.Entry<ShardId, ShardUpsertRequest>> it = requestsByShard.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<ShardId, ShardUpsertRequest> entry = it.next();
                ShardUpsertRequest shardUpsertRequest = entry.getValue();
                //shardUpdateRequest.timeout(requestTimeout);
                execute(shardUpsertRequest);
                it.remove();
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
        } finally {
            counter.set(0);
            executeLock.release();
        }
    }

    private void execute(ShardUpsertRequest updateRequest) {
        trace(String.format("execute shard request %d", updateRequest.shardId()));
        transportShardUpsertActionDelegate.execute(updateRequest, new ResponseListener(updateRequest));
    }

    private void doRetry(final ShardUpsertRequest request, final boolean repeatingRetry) {
        trace("doRetry");
        if (repeatingRetry) {
            try {
                Thread.sleep(currentDelay.incrementAndGet());
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            transportShardUpsertActionDelegate.execute(request, new RetryResponseListener(request));
        } else {
            // new retries will be spawned in new thread because they can block
            scheduledExecutorService.schedule(
                    new Runnable() {
                        @Override
                        public void run() {
                            // will block if other retries/writer are active
                            try {
                                retryLock.writeLock();
                            } catch (InterruptedException e) {
                                Thread.interrupted();
                            }
                            transportShardUpsertActionDelegate.execute(request, new RetryResponseListener(request));
                        }
                    }, currentDelay.getAndIncrement(), TimeUnit.MILLISECONDS);
        }
    }

    private void createIndexIfRequired(final String indexName) {
        if (!indicesCreated.contains(indexName) || autoCreateIndex.shouldAutoCreate(indexName, clusterService.state())) {
            try {
                transportCreateIndexAction.execute(new CreateIndexRequest(indexName).cause("bulkShardProcessor")).actionGet();
                indicesCreated.add(indexName);
            } catch (Throwable e) {
                e = ExceptionsHelper.unwrapCause(e);
                if (e instanceof IndexAlreadyExistsException) {
                    // copy from with multiple readers might attempt to create the index
                    // multiple times
                    // can be ignored.
                    if (logger.isTraceEnabled()) {
                        logger.trace("copy from index {}", e.getMessage());
                    }
                    indicesCreated.add(indexName);
                } else {
                    setFailure(e);
                }
            }
        }
    }

    private void processResponse(ShardUpsertResponse shardUpsertResponse) {
        trace("execute response");
        for (int i = 0; i < shardUpsertResponse.locations().size(); i++) {
            int location = shardUpsertResponse.locations().get(i);
            synchronized (responsesLock) {
                responses.set(location, shardUpsertResponse.responses().get(i) != null);
            }
        }

        setResultIfDone(shardUpsertResponse.locations().size());
    }

    private void processFailure(Throwable e, ShardUpsertRequest shardUpsertRequest, boolean repeatingRetry) {
        trace("execute failure");
        e = Exceptions.unwrap(e);
        if (e instanceof EsRejectedExecutionException) {
            logger.trace("{}, retrying", e.getMessage());
            doRetry(shardUpsertRequest, repeatingRetry);
        } else {
            if (repeatingRetry) {
                // release failed retry
                try {
                    retryLock.writeUnlock();
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
            for (IntCursor intCursor : shardUpsertRequest.locations()) {
                synchronized (responsesLock) {
                    responses.set(intCursor.value, false);
                }
            }
            setFailure(e);
        }
    }

    /**
     * Process update and insert assignments to extract data types and define which row values
     * must be streamed.
     */
    public static AssignmentVisitorContext processAssignments(@Nullable Map<Reference, Symbol> updateAssignments,
                                                              @Nullable Map<Reference, Symbol> insertAssignments) {
        AssignmentVisitorContext context = new AssignmentVisitorContext();
        if (updateAssignments != null) {
            for (Symbol symbol : updateAssignments.values()) {
                assignmentVisitor.process(symbol, context);
            }
        }
        if (insertAssignments != null) {
            for (Symbol symbol : insertAssignments.values()) {
                assignmentVisitor.process(symbol, context);
            }
        }

        Collections.sort(context.inputColumns, new Comparator<InputColumn>() {
            @Override
            public int compare(InputColumn o1, InputColumn o2) {
                return Integer.compare(o1.index(), o2.index());
            }
        });

        context.dataTypes = new DataType[context.inputColumns.size()];
        for (int i = 0; i < context.inputColumns.size(); i++) {
            context.dataTypes[i] = context.inputColumns.get(i).valueType();
        }

        return context;
    }

    class ResponseListener implements ActionListener<ShardUpsertResponse> {

        protected final ShardUpsertRequest shardUpsertRequest;

        public ResponseListener(ShardUpsertRequest shardUpsertRequest) {
            this.shardUpsertRequest = shardUpsertRequest;
        }

        @Override
        public void onResponse(ShardUpsertResponse shardUpsertResponse) {
            processResponse(shardUpsertResponse);
        }

        @Override
        public void onFailure(Throwable e) {
            processFailure(e, shardUpsertRequest, false);
        }
    }

    private void trace(String message) {
        if (logger.isTraceEnabled()) {
            logger.trace("BulkShardProcessor: pending: {}; active retries: {} - {}",
                    pending.get(), retryLock.activeWriters(), message);
        }
    }

    class RetryResponseListener extends ResponseListener {

        public RetryResponseListener(ShardUpsertRequest shardUpsertRequest) {
            super(shardUpsertRequest);
        }

        @Override
        public void onResponse(ShardUpsertResponse shardUpsertResponse) {
            trace("BulkShardProcessor retry success");
            currentDelay.set(0);
            try {
                retryLock.writeUnlock();
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            processResponse(shardUpsertResponse);
        }

        @Override
        public void onFailure(Throwable e) {
            trace("BulkShardProcessor retry failure");
            processFailure(e, shardUpsertRequest, true);
        }
    }

    /**
     * A {@link java.util.concurrent.Semaphore} based read/write lock allowing multiple readers,
     * no reader will block others, and only 1 active writer. Writers take
     * precedence over readers, a writer will block all readers.
     * Compared to a {@link ReadWriteLock}, no lock is owned by a thread.
     */
    class ReadWriteLock {
        private final Semaphore readLock = new Semaphore(1, true);
        private final Semaphore writeLock = new Semaphore(1, true);
        private final AtomicInteger activeWriters = new AtomicInteger(0);
        private final AtomicInteger waitingReaders = new AtomicInteger(0);

        public ReadWriteLock() {
        }

        public void writeLock() throws InterruptedException {
            // check readLock permits to prevent deadlocks
            if (activeWriters.getAndIncrement() == 0 && readLock.availablePermits() == 1) {
                // draining read permits, so all reads will block
                readLock.drainPermits();
            }
            writeLock.acquire();
        }

        public void writeUnlock() throws InterruptedException {
            if (activeWriters.decrementAndGet() == 0) {
                // unlock all readers
                readLock.release(waitingReaders.getAndSet(0)+1);
            }
            writeLock.release();
        }

        public void readLock() throws InterruptedException {
            // only acquire permit if writers are active
            if(activeWriters.get() > 0) {
                waitingReaders.getAndIncrement();
                readLock.acquire();
            }
        }

        public int activeWriters() {
            return activeWriters.get();
        }

    }

    public static class AssignmentVisitorContext {

        private List<InputColumn> inputColumns = new ArrayList<>();
        private List<Integer> indices = new ArrayList<>();
        private DataType[] dataTypes;

        public List<Integer> columnIndicesToStream() {
            return indices;
        }

        public DataType[] dataTypes() {
            return dataTypes;
        }
    }

    static class AssignmentVisitor extends SymbolVisitor<AssignmentVisitorContext, Symbol> {

        @Override
        public Symbol visitInputColumn(InputColumn inputColumn, AssignmentVisitorContext context) {
            if (!context.inputColumns.contains(inputColumn)) {
                context.inputColumns.add(inputColumn);
                context.indices.add(inputColumn.index());
            }
            return inputColumn;
        }

        @Override
        public Symbol visitFunction(Function symbol, AssignmentVisitorContext context) {
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            return symbol;
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, AssignmentVisitorContext context) {
            return symbol;
        }
    }

}
