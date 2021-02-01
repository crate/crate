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
