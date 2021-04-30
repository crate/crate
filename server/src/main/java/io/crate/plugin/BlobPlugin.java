/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import io.crate.blob.BlobModule;
import io.crate.blob.BlobService;
import io.crate.blob.DeleteBlobAction;
import io.crate.blob.PutChunkAction;
import io.crate.blob.StartBlobAction;
import io.crate.blob.TransportDeleteBlobAction;
import io.crate.blob.TransportPutChunkAction;
import io.crate.blob.TransportStartBlobAction;
import io.crate.blob.v2.BlobIndicesModule;
import io.crate.blob.v2.BlobIndicesService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class BlobPlugin extends Plugin implements ActionPlugin {

    private final Settings settings;

    public BlobPlugin(Settings settings) {
        this.settings = settings;
    }

    public String name() {
        return "blob";
    }

    public String description() {
        return "Plugin that adds BLOB support to CrateDB";
    }

    @Override
    public Collection<Module> createGuiceModules() {
        Collection<Module> modules = new ArrayList<>(2);
        modules.add(new BlobModule());
        if (Node.NODE_DATA_SETTING.get(settings)) {
            // the actual blob indices module is only available on data nodes. the blobservice on non data nodes will
            // handle the requests redirection to data nodes.
            modules.add(new BlobIndicesModule());
        }
        return modules;
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
        return List.of(BlobService.class);
    }

    @Override
    public List<ActionHandler<? extends TransportRequest, ? extends TransportResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(PutChunkAction.INSTANCE, TransportPutChunkAction.class),
            new ActionHandler<>(StartBlobAction.INSTANCE, TransportStartBlobAction.class),
            new ActionHandler<>(DeleteBlobAction.INSTANCE, TransportDeleteBlobAction.class));
    }
}
