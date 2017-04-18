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

package io.crate.plugin;


import com.google.common.collect.ImmutableList;
import io.crate.blob.*;
import io.crate.blob.v2.BlobIndicesModule;
import io.crate.blob.v2.BlobIndicesService;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.*;

import static org.elasticsearch.common.network.NetworkModule.HTTP_TYPE_KEY;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;

public class BlobPlugin extends Plugin implements ActionPlugin {

    private static final String CRATE_HTTP_TRANSPORT_NAME = "crate";

    private final Settings settings;

    public BlobPlugin(Settings settings) {
        this.settings = settings;
    }

    public String name() {
        return "blob";
    }

    public String description() {
        return "plugin that adds BlOB support to crate";
    }


    @Override
    public Collection<Module> createGuiceModules() {
        if (settings.getAsBoolean("node.client", false)) {
            return Collections.emptyList();
        }
        Collection<Module> modules = new ArrayList<>(2);
        modules.add(new BlobModule());
        modules.add(new BlobIndicesModule());
        return modules;
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
            .put(HTTP_TYPE_KEY, CRATE_HTTP_TRANSPORT_NAME)
            .put(SETTING_HTTP_COMPRESSION.getKey(), false)
            .build();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            BlobIndicesService.SETTING_BLOBS_PATH,
            BlobIndicesService.SETTING_INDEX_BLOBS_ENABLED,
            BlobIndicesService.SETTING_INDEX_BLOBS_PATH
        );
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        // only start the service if we have a data node
        if (settings.getAsBoolean("node.client", false)) {
            return Collections.emptyList();
        }
        return ImmutableList.of(BlobService.class);
    }

    // FIXME migrate to new plugin infrastructure
//    public void onModule(NetworkModule networkModule) {
//        if (networkModule.canRegisterHttpExtensions()) {
//            networkModule.registerHttpTransport(CRATE_HTTP_TRANSPORT_NAME, CrateNettyHttpServerTransport.class);
//        }
//    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(PutChunkAction.INSTANCE, TransportPutChunkAction.class),
            new ActionHandler<>(StartBlobAction.INSTANCE, TransportStartBlobAction.class),
            new ActionHandler<>(DeleteBlobAction.INSTANCE, TransportDeleteBlobAction.class));
    }
}
