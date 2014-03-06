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

package io.crate.plugin.inout;

import java.util.Collection;

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

import io.crate.module.dump.DumpModule;
import io.crate.module.export.ExportModule;
import io.crate.module.import_.ImportModule;
import io.crate.module.reindex.ReindexModule;
import io.crate.module.restore.RestoreModule;
import io.crate.module.searchinto.SearchIntoModule;
import io.crate.rest.action.admin.dump.RestDumpAction;
import io.crate.rest.action.admin.export.RestExportAction;
import io.crate.rest.action.admin.import_.RestImportAction;
import io.crate.rest.action.admin.reindex.RestReindexAction;
import io.crate.rest.action.admin.restore.RestRestoreAction;
import io.crate.rest.action.admin.searchinto.RestSearchIntoAction;

public class InOutPlugin extends AbstractPlugin {

    private final Settings settings;

    public InOutPlugin(Settings settings) {
        this.settings = settings;
    }

    public String name() {
        return "inout";
    }

    public String description() {
        return "InOut plugin";
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(RestExportAction.class);
        restModule.addRestAction(RestImportAction.class);
        restModule.addRestAction(RestSearchIntoAction.class);
        restModule.addRestAction(RestDumpAction.class);
        restModule.addRestAction(RestRestoreAction.class);
        restModule.addRestAction(RestReindexAction.class);
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        if (!settings.getAsBoolean("node.client", false)) {
            modules.add(ExportModule.class);
            modules.add(ImportModule.class);
            modules.add(SearchIntoModule.class);
            modules.add(DumpModule.class);
            modules.add(RestoreModule.class);
            modules.add(ReindexModule.class);
        }
        return modules;
    }
}
