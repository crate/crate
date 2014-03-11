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

package io.crate.action.sql;

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.*;
import io.crate.blob.v2.BlobIndices;
import org.elasticsearch.common.inject.Inject;

public class DDLAnalysisDispatcher extends AnalysisVisitor<Void, ListenableFuture<Void>> {

    private final BlobIndices blobIndices;

    @Inject
    private DDLAnalysisDispatcher(BlobIndices blobIndices) {
        this.blobIndices = blobIndices;
    }

    @Override
    protected ListenableFuture<Void> visitAnalysis(Analysis analysis, Void context) {
        throw new UnsupportedOperationException(String.format("Can't handle \"%s\"", analysis));
    }

    @Override
    public ListenableFuture<Void> visitCreateBlobTableAnalysis(
            CreateBlobTableAnalysis analysis, Void context) {
        return blobIndices.createBlobTable(
                analysis.tableName(), analysis.numberOfReplicas(), analysis.numberOfShards());
    }

    @Override
    public ListenableFuture<Void> visitDropBlobTableAnalysis(DropBlobTableAnalysis analysis, Void context) {
        return blobIndices.dropBlobTable(analysis.table().ident().name());
    }
}
