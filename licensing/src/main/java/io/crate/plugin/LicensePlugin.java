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

import io.crate.license.LicenseKey;
import io.crate.license.LicenseModule;
import io.crate.license.LicenseService;
import io.crate.es.cluster.NamedDiff;
import io.crate.es.cluster.metadata.MetaData;
import io.crate.es.common.ParseField;
import io.crate.es.common.component.LifecycleComponent;
import io.crate.es.common.inject.Module;
import io.crate.es.common.io.stream.NamedWriteableRegistry;
import io.crate.es.common.settings.Setting;
import io.crate.es.common.xcontent.NamedXContentRegistry;
import io.crate.es.plugins.ActionPlugin;
import io.crate.es.plugins.Plugin;

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
        return Collections.emptyList();
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.singletonList(LicenseService.class);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedWriteableRegistry.Entry(
            MetaData.Custom.class,
            LicenseKey.WRITEABLE_TYPE,
            LicenseKey::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            LicenseKey.WRITEABLE_TYPE,
            in -> LicenseKey.readDiffFrom(MetaData.Custom.class, LicenseKey.WRITEABLE_TYPE, in)
        ));
        return entries;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Collections.singletonList(new NamedXContentRegistry.Entry(
            MetaData.Custom.class,
            new ParseField(LicenseKey.WRITEABLE_TYPE),
            LicenseKey::fromXContent
        ));
    }
}
