/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport.task.inout;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.planner.node.dml.CopyNode;
import io.crate.Constants;
import io.crate.action.import_.ImportRequest;
import io.crate.action.import_.ImportResponse;
import io.crate.action.import_.NodeImportResponse;
import io.crate.action.import_.TransportImportAction;
import io.crate.import_.Importer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ImportTask implements Task<Object[][]> {

    static class ImportListener implements ActionListener<ImportResponse> {

        private final SettableFuture<Object[][]> future;

        ImportListener(SettableFuture<Object[][]> future) {
            this.future = future;
        }

        @Override
        public void onResponse(ImportResponse nodeImportResponses) {
            long rowCount = 0;
            for (NodeImportResponse nodeImportResponse : nodeImportResponses.getResponses()) {
                for (Importer.ImportCounts importCounts : nodeImportResponse.result().importCounts) {
                    rowCount += importCounts.successes.get();
                }
            }
            this.future.set(new Object[][]{new Object[]{rowCount}});
        }

        @Override
        public void onFailure(Throwable e) {
            this.future.setException(e);
        }
    }

    private final TransportImportAction transport;
    private final ImportListener listener;
    private final SettableFuture<Object[][]> result;
    private final List<ListenableFuture<Object[][]>> results;
    private final ImportRequest request;

    public ImportTask(TransportImportAction transport, CopyNode copyNode) {
        this.transport = transport;
        this.result = SettableFuture.create();
        this.results = Arrays.<ListenableFuture<Object[][]>>asList(this.result);
        this.request = buildImportRequest(copyNode);
        this.listener = new ImportListener(this.result);
    }

    private ImportRequest buildImportRequest(CopyNode copyNode) {
        ImportRequest request = new ImportRequest();
        BytesReference source = null;
        try {
            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
            source = jsonBuilder.startObject()
                    .field("path", copyNode.path())
                    .endObject().bytes();
        } catch (IOException e) {
            this.result.setException(e);
        }
        request.source(source, false);

        request.index(copyNode.index());
        request.type(Constants.DEFAULT_MAPPING_TYPE);
        return request;
    }

    @Override
    public void start() {
        this.transport.execute(request, listener);
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return this.results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        throw new UnsupportedOperationException();
    }
}
