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

package io.crate.executor.transport.task.elasticsearch.facet;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Scorer;
import io.crate.Constants;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

/**
 * A Collector which generates elasticsearch update requests for every document it collects
 */
public class UpdateCollector extends FacetExecutor.Collector {

    private final SearchLookup lookup;
    private final TransportUpdateAction updateAction;
    private final Map<String, Object> updateDoc;
    private final ShardId shardId;
    private long rowCount;
    private Long requiredVersion;
    private AtomicReaderContext currentReaderContext;

    public long rowCount() {
        return rowCount;
    }

    class CollectorUpdateRequest extends UpdateRequest {

        CollectorUpdateRequest(ShardId shardId, Uid uid) {
            super(shardId.getIndex(), uid.type(), uid.id());
            this.shardId = shardId.id();
            retryOnConflict(Constants.UPDATE_RETRY_ON_CONFLICT);
            paths(updateDoc);
        }
    }

    public UpdateCollector(
            Map<String, Object> doc,
            Long requiredVersion,
            TransportUpdateAction updateAction,
            SearchContext context
            ) {
        this.shardId = context.indexShard().shardId();
        this.updateAction = updateAction;
        this.lookup = context.lookup();
        this.updateDoc = doc;
        this.rowCount = 0;
        this.requiredVersion = requiredVersion;
    }

    @Override
    public void setScorer(Scorer scorer) {
        lookup.setScorer(scorer);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        this.currentReaderContext = context;
        lookup.setNextReader(context);
    }


    @Override
    public void postCollection() {
        // nothing to do here
    }

    @Override
    public void collect(int doc) throws IOException {
        lookup.setNextDocId(doc);
        Uid uid = Uid.createUid(((ScriptDocValues.Strings) lookup.doc().get("_uid")).getValue());
        if (requiredVersion != null) {
            Long currentVersion = Versions.loadVersion(
                    currentReaderContext.reader(),
                    new Term(UidFieldMapper.NAME, uid.toBytesRef())
            );
            if (!currentVersion.equals(requiredVersion)) {
                return;
            }
        }
        collect(uid);
    }

    private void collect(Uid uid) {
        UpdateRequest request = new CollectorUpdateRequest(shardId, uid);
        // Since we are sequential here, it should be ok to reuse the same request instance
        UpdateResponse response = updateAction.execute(request).actionGet();
        rowCount++;
    }
}
