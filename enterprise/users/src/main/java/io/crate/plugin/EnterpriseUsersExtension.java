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

import io.crate.metadata.UsersMetadata;
import io.crate.metadata.UsersPrivilegesMetadata;
import io.crate.scalar.UsersScalarFunctionModule;
import io.crate.user.UserExtension;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class EnterpriseUsersExtension implements UserExtension {

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(4);
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            UsersMetadata.TYPE,
            UsersMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            UsersMetadata.TYPE,
            in -> UsersMetadata.readDiffFrom(Metadata.Custom.class, UsersMetadata.TYPE, in)
        ));

        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            UsersPrivilegesMetadata.TYPE,
            UsersPrivilegesMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            UsersPrivilegesMetadata.TYPE,
            in -> UsersPrivilegesMetadata.readDiffFrom(Metadata.Custom.class, UsersPrivilegesMetadata.TYPE, in)
        ));
        return entries;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(2);
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(UsersMetadata.TYPE),
            UsersMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(UsersPrivilegesMetadata.TYPE),
            UsersPrivilegesMetadata::fromXContent
        ));
        return entries;
    }

    @Override
    public Collection<Module> getModules(Settings settings) {
        return Arrays.asList(
            new UserManagementModule(),
            new AuthenticationModule(settings),
            new UsersScalarFunctionModule());
    }
}
