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

package org.cratedb.plugin;

import org.cratedb.module.CrateCoreModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.cratedb.rest.CrateRestMainAction;

import java.util.ArrayList;
import java.util.Collection;

public class CrateCorePlugin extends AbstractPlugin {

    private final Settings settings;

    public CrateCorePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "crate-core";
    }

    @Override
    public String description() {
        return "plugin that provides a collection of utilities used in other crate modules.";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = new ArrayList<>();
        if (!settings.getAsBoolean("node.client", false)) {
            modules.add(CrateCoreModule.class);
        }
        return modules;
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(CrateRestMainAction.class);
    }
}
