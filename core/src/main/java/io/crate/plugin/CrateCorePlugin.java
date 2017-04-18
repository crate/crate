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

package io.crate.plugin;

import io.crate.ClusterIdService;
import io.crate.module.CrateCoreModule;
import io.crate.rest.CrateRestHandlerWrapper;
import io.crate.rest.CrateRestMainAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public class CrateCorePlugin extends Plugin implements ActionPlugin {

    private final Settings settings;
    private final IndexEventListenerProxy indexEventListenerProxy;

    public CrateCorePlugin(Settings settings) {
        this.settings = settings;
        this.indexEventListenerProxy = new IndexEventListenerProxy();
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.singletonList(ClusterIdService.class);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(CrateRestHandlerWrapper.ES_API_ENABLED_SETTING);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return Collections.singletonList(new CrateCoreModule(settings, indexEventListenerProxy));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
                                             ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        // FIXME cluster service needs to be injected here
        return Collections.singletonList(new CrateRestMainAction(settings, restController, null));
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        indexModule.addIndexEventListener(indexEventListenerProxy);
    }

    @Override
    public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
        return restHandler -> new CrateRestHandlerWrapper(settings, restHandler);
    }
}
