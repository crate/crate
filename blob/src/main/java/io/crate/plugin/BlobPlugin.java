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


import io.crate.blob.*;
import io.crate.blob.v2.BlobIndicesModule;
import io.crate.http.netty.CrateNettyHttpServerTransport;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class BlobPlugin extends Plugin {

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
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        // only start the service if we have a data node
        if (settings.getAsBoolean("node.client", false)) {
            return Collections.emptyList();
        }
        return Collections.singletonList(BlobService.class);
    }

    public void onModule(HttpServerModule module) {
        module.setHttpServerTransport(CrateNettyHttpServerTransport.class, "crate");
    }

    public void onModule(ActionModule module) {
        module.registerAction(PutChunkAction.INSTANCE, TransportPutChunkAction.class);
        module.registerAction(StartBlobAction.INSTANCE, TransportStartBlobAction.class);
        module.registerAction(DeleteBlobAction.INSTANCE, TransportDeleteBlobAction.class);
    }
}
