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
import io.crate.metadata.CustomMetaDataUpgraderLoader;
import io.crate.module.CrateCoreModule;
import io.crate.rest.CrateRestFilter;
import io.crate.rest.CrateRestMainAction;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
        return Collections.singletonList(CrateRestFilter.ES_API_ENABLED_SETTING);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return Collections.singletonList(new CrateCoreModule(settings, indexEventListenerProxy));
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        return Collections.singletonList(CrateRestMainAction.class);
    }

    @Override
    public UnaryOperator<Map<String, MetaData.Custom>> getCustomMetaDataUpgrader() {
        return new CustomMetaDataUpgraderLoader(settings);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        indexModule.addIndexEventListener(indexEventListenerProxy);
    }
}
