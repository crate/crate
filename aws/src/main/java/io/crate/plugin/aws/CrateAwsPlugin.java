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

package io.crate.plugin.aws;

import com.google.common.collect.Lists;
import io.crate.plugin.AbstractPlugin;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.cloud.aws.CloudAwsPlugin;
import org.elasticsearch.repositories.RepositoriesModule;

import java.util.Collection;
import java.util.List;

public class CrateAwsPlugin extends AbstractPlugin {

    private final CloudAwsPlugin esAwsPlugin;

    public CrateAwsPlugin(Settings settings) {
        this.esAwsPlugin = new CloudAwsPlugin(settings);
    }

    @Override
    public String name() {
        return "crate-aws";
    }

    @Override
    public String description() {
        return "crate on aws";
    }

    @Override
    public Collection<Module> modules(Settings settings) {
        List<Module> modules = Lists.newArrayList(super.modules(settings));
        modules.add(new S3RepositoryAnalysisModule(settings));
        modules.addAll(esAwsPlugin.modules(settings));
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        return esAwsPlugin.services();
    }

    public void onModule(RepositoriesModule repositoriesModule) {
        esAwsPlugin.onModule(repositoriesModule);
    }
}
