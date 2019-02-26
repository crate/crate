/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.es.action.support.master;

import io.crate.es.action.ActionResponse;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.io.stream.Writeable;
import io.crate.es.common.settings.Setting;
import io.crate.es.common.settings.Setting.Property;
import io.crate.es.common.settings.Settings;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

import java.util.function.Supplier;

/**
 * A base class for read operations that needs to be performed on the master node.
 * Can also be executed on the local node if needed.
 */
public abstract class TransportMasterNodeReadAction<Request extends MasterNodeReadRequest<Request>, Response extends ActionResponse>
        extends TransportMasterNodeAction<Request, Response> {

    public static final Setting<Boolean> FORCE_LOCAL_SETTING =
        Setting.boolSetting("action.master.force_local", false, Property.NodeScope);

    private final boolean forceLocal;

    protected TransportMasterNodeReadAction(Settings settings, String actionName, TransportService transportService,
                                            ClusterService clusterService, ThreadPool threadPool,
                                            IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request) {
        this(settings, actionName, true, transportService, clusterService, threadPool, indexNameExpressionResolver,request);
    }

    protected TransportMasterNodeReadAction(Settings settings, String actionName, TransportService transportService,
                                            ClusterService clusterService, ThreadPool threadPool,
                                            Writeable.Reader<Request> request, IndexNameExpressionResolver indexNameExpressionResolver) {
        this(settings, actionName, true, transportService, clusterService, threadPool, request,
            indexNameExpressionResolver);
    }

    protected TransportMasterNodeReadAction(Settings settings, String actionName, boolean checkSizeLimit, TransportService transportService,
                                            ClusterService clusterService, ThreadPool threadPool,
                                            IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request) {
        super(settings, actionName, checkSizeLimit, transportService, clusterService, threadPool,
            indexNameExpressionResolver,request);
        this.forceLocal = FORCE_LOCAL_SETTING.get(settings);
    }

    protected TransportMasterNodeReadAction(Settings settings, String actionName, boolean checkSizeLimit, TransportService transportService,
                                            ClusterService clusterService, ThreadPool threadPool,
                                            Writeable.Reader<Request> request, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, actionName, checkSizeLimit, transportService, clusterService, threadPool, request,
            indexNameExpressionResolver);
        this.forceLocal = FORCE_LOCAL_SETTING.get(settings);
    }

    @Override
    protected final boolean localExecute(Request request) {
        return forceLocal || request.local();
    }
}
