/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.analyze.where.DocKeys;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.exceptions.Exceptions;
import io.crate.executor.JobTask;
import io.crate.jobs.ESJobContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.RowReceivers;
import io.crate.planner.node.dml.ESDelete;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class ESDeleteTask extends JobTask {

    protected final List<SettableFuture<Long>> results;
    private final ESDelete esDelete;
    private final JobContextService jobContextService;
    protected JobExecutionContext.Builder builder;

    public ESDeleteTask(ESDelete esDelete,
                        TransportDeleteAction transport,
                        JobContextService jobContextService) {
        super(esDelete.jobId());
        this.esDelete = esDelete;
        this.jobContextService = jobContextService;

        results = new ArrayList<>(esDelete.getBulkSize());
        for (int i = 0; i < esDelete.getBulkSize(); i++) {
            SettableFuture<Long> result = SettableFuture.create();
            results.add(result);
        }

        List<DeleteRequest> requests = new ArrayList<>(esDelete.docKeys().size());
        List<ActionListener> listeners = new ArrayList<>(esDelete.docKeys().size());
        int resultIdx = 0;
        for (DocKeys.DocKey docKey : esDelete.docKeys()) {
            DeleteRequest request = new DeleteRequest(
                ESGetTask.indexName(esDelete.tableInfo(), docKey.partitionValues().orNull()),
                Constants.DEFAULT_MAPPING_TYPE, docKey.id());
            request.routing(docKey.routing());
            if (docKey.version().isPresent()) {
                //noinspection OptionalGetWithoutIsPresent
                request.version(docKey.version().get());
            }
            requests.add(request);
            SettableFuture<Long> result = results.get(esDelete.getItemToBulkIdx().get(resultIdx));
            listeners.add(new DeleteResponseListener(result));
            resultIdx++;
        }

        for (int i = 0; i < results.size(); i++) {
            if (!esDelete.getItemToBulkIdx().values().contains(i)) {
                results.get(i).set(0L);
            }
        }
        createContextBuilder("delete", requests, listeners, transport);
    }

    private void createContextBuilder(String operationName,
                                      List<? extends ActionRequest> requests,
                                      List<? extends ActionListener> listeners,
                                      TransportAction transportAction) {
        ESJobContext esJobContext = new ESJobContext(esDelete.executionPhaseId(), operationName,
            requests, listeners, results, transportAction);
        builder = jobContextService.newBuilder(jobId());
        builder.addSubContext(esJobContext);
    }

    private void startContext() throws Throwable {
        assert builder != null : "Context must be created first";
        JobExecutionContext ctx = jobContextService.createContext(builder);
        ctx.start();
    }

    @Override
    public void execute(final RowReceiver rowReceiver, Row parameters) {
        SettableFuture<Long> result = results.get(0);
        try {
            startContext();
        } catch (Throwable throwable) {
            rowReceiver.fail(throwable);
            return;
        }
        Futures.addCallback(result, new FutureCallback<Long>() {
            @Override
            public void onSuccess(@Nullable Long result) {
                RowReceivers.sendOneRow(rowReceiver, new Row1(result));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                rowReceiver.fail(t);
            }
        });
    }


    private static class DeleteResponseListener implements ActionListener<DeleteResponse> {

        private final SettableFuture<Long> result;

        DeleteResponseListener(SettableFuture<Long> result) {
            this.result = result;
        }

        @Override
        public void onResponse(DeleteResponse response) {
            if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                result.set(0L);
            } else {
                result.set(1L);
            }
        }

        @Override
        public void onFailure(Exception e) {
            Throwable t = Exceptions.unwrap(e); // unwrap to get rid of RemoteTransportException
            if (t instanceof VersionConflictEngineException) {
                // treat version conflict as rows affected = 0
                result.set(0L);
            } else {
                result.setException(t);
            }
        }
    }

    @Override
    public final ListenableFuture<List<Long>> executeBulk() {
        try {
            startContext();
        } catch (Throwable throwable) {
            return Futures.immediateFailedFuture(throwable);
        }
        return Futures.successfulAsList(results);
    }


}
