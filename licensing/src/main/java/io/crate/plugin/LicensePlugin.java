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

import io.crate.license.LicenseMetaData;
import io.crate.license.LicenseModule;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class LicensePlugin extends Plugin implements ActionPlugin {

    public String name() {
        return "license";
    }

    public String description() {
        return "Plugin that adds licensing support to CrateDB";
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return Collections.singletonList(new LicenseModule());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedWriteableRegistry.Entry(
            MetaData.Custom.class,
            LicenseMetaData.TYPE,
            LicenseMetaData::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            LicenseMetaData.TYPE,
            in -> LicenseMetaData.readDiffFrom(MetaData.Custom.class, LicenseMetaData.TYPE, in)
        ));
        return entries;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Collections.singletonList(new NamedXContentRegistry.Entry(
            MetaData.Custom.class,
            new ParseField(LicenseMetaData.TYPE),
            LicenseMetaData::fromXContent
        ));
    }
}
