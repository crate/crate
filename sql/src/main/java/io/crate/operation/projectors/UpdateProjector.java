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

package io.crate.operation.projectors;

import io.crate.Constants;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class UpdateProjector implements Projector {

    private Projector downstream;
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicReference<Throwable> upstreamFailure = new AtomicReference<>(null);
    private final ArrayList<ActionFuture<UpdateResponse>> updateRequests = new ArrayList<>();

    private final ShardId shardId;
    private final TransportUpdateAction transportUpdateAction;
    private final List<ColumnIdent> columnIdents;
    private final CollectExpression<?>[] collectExpressions;
    @Nullable
    private final Long requiredVersion;
    private final Object lock = new Object();

    public UpdateProjector(ShardId shardId,
                           TransportUpdateAction transportUpdateAction,
                           List<ColumnIdent> columnIdents,
                           CollectExpression<?>[] collectExpressions,
                           @Nullable Long requiredVersion) {
        this.shardId = shardId;
        this.transportUpdateAction = transportUpdateAction;
        this.columnIdents = columnIdents;
        this.collectExpressions = collectExpressions;
        this.requiredVersion = requiredVersion;

    }

    @Override
    public void startProjection() {
        if (remainingUpstreams.get() <= 0) {
            upstreamFinished();
        }
    }

    @Override
    public boolean setNextRow(Object... row) {
        synchronized (lock) {
            for (CollectExpression<?> collectExpression : collectExpressions) {
                collectExpression.setNextRow(row);
            }

            updateRequests.add(transportUpdateAction.execute(updateRequest()));
        }

        return true;
    }

    private UpdateRequest updateRequest() {
        Map<String, Object> updateDoc = new HashMap<>(columnIdents.size());

        // first input is always the doc's uid
        Uid uid = Uid.createUid(((BytesRef)collectExpressions[0].value()).utf8ToString());

        for (int i = 0; i < columnIdents.size(); i++) {
            updateDoc.put(columnIdents.get(i).fqn(), collectExpressions[i+1].value());
        }

        return new ProjectorUpdateRequest(shardId, uid, updateDoc);
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        remainingUpstreams.incrementAndGet();
    }

    @Override
    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }

        if (downstream != null) {
            try {
                long rowCount = 0;
                for (ActionFuture<UpdateResponse> updateResponse : updateRequests) {
                    updateResponse.get();
                    rowCount++;
                }
                downstream.setNextRow(rowCount);
            } catch (Exception e) {
                downstream.upstreamFailed(e);
                return;
            }
            Throwable throwable = upstreamFailure.get();
            if (throwable == null) {
                downstream.upstreamFinished();
            } else {
                downstream.upstreamFailed(throwable);
            }
        }
    }

    @Override
    public void upstreamFailed(Throwable throwable) {
        upstreamFailure.set(throwable);
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }
        if (downstream != null) {
            try {
                long rowCount = 0;
                for (ActionFuture<UpdateResponse> updateResponse : updateRequests) {
                    updateResponse.get();
                    rowCount++;
                }
                downstream.setNextRow(rowCount);
            } catch (Exception e) {
            }
            downstream.upstreamFailed(throwable);
        }
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        downstream.registerUpstream(this);
    }

    class ProjectorUpdateRequest extends UpdateRequest {

        ProjectorUpdateRequest(ShardId shardId, Uid uid, Map<String, Object> updateDoc) {
            super(shardId.getIndex(), uid.type(), uid.id());
            this.shardId = shardId.id();
            if (requiredVersion != null) {
                version(requiredVersion);
            } else {
                retryOnConflict(Constants.UPDATE_RETRY_ON_CONFLICT);
            }
            paths(updateDoc);
        }
    }
}
