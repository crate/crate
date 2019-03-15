/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.plugin;

import io.crate.license.EnterpriseLicenseComponent;
import io.crate.license.LicenseExtension;
import io.crate.license.LicenseKey;
import io.crate.license.LicenseModule;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class EnterpriseLicenseExtension implements LicenseExtension {

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

    @Override
    public Collection<Module> getModules(Settings settings) {
        return Collections.singletonList(new LicenseModule());
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.singletonList(EnterpriseLicenseComponent.class);
    }
}
