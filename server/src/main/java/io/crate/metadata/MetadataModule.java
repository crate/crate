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

package io.crate.metadata;


import static org.elasticsearch.cluster.AbstractNamedDiffable.readDiffFrom;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;

import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.fdw.ForeignTablesMetadata;
import io.crate.fdw.ServersMetadata;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;

public class MetadataModule extends AbstractModule {

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            UserDefinedFunctionsMetadata.TYPE,
            UserDefinedFunctionsMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            ViewsMetadata.TYPE,
            ViewsMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            UserDefinedFunctionsMetadata.TYPE,
            in -> readDiffFrom(Metadata.Custom.class, UserDefinedFunctionsMetadata.TYPE, in)
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            ViewsMetadata.TYPE,
            in -> readDiffFrom(Metadata.Custom.class, ViewsMetadata.TYPE, in)
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            UsersMetadata.TYPE,
            UsersMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            UsersMetadata.TYPE,
            in -> readDiffFrom(Metadata.Custom.class, UsersMetadata.TYPE, in)
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            RolesMetadata.TYPE,
            RolesMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            RolesMetadata.TYPE,
            in -> readDiffFrom(Metadata.Custom.class, RolesMetadata.TYPE, in)
        ));

        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            UsersPrivilegesMetadata.TYPE,
            UsersPrivilegesMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            UsersPrivilegesMetadata.TYPE,
            in -> readDiffFrom(Metadata.Custom.class, UsersPrivilegesMetadata.TYPE, in)
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            PublicationsMetadata.TYPE,
            PublicationsMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            PublicationsMetadata.TYPE,
            in -> readDiffFrom(Metadata.Custom.class, PublicationsMetadata.TYPE, in)
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            SubscriptionsMetadata.TYPE,
            SubscriptionsMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            SubscriptionsMetadata.TYPE,
            in -> readDiffFrom(Metadata.Custom.class, SubscriptionsMetadata.TYPE, in)
        ));


        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            ServersMetadata.TYPE,
            ServersMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            ServersMetadata.TYPE,
            in -> readDiffFrom(Metadata.Custom.class, ServersMetadata.TYPE, in)
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            ForeignTablesMetadata.TYPE,
            ForeignTablesMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            ForeignTablesMetadata.TYPE,
            in -> readDiffFrom(Metadata.Custom.class, ForeignTablesMetadata.TYPE, in)
        ));


        return entries;
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContents(NodeContext nodeCtx) {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(UserDefinedFunctionsMetadata.TYPE),
            UserDefinedFunctionsMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(ViewsMetadata.TYPE),
            ViewsMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(UsersMetadata.TYPE),
            UsersMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(RolesMetadata.TYPE),
            RolesMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(UsersPrivilegesMetadata.TYPE),
            UsersPrivilegesMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(PublicationsMetadata.TYPE),
            PublicationsMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(SubscriptionsMetadata.TYPE),
            SubscriptionsMetadata::fromXContent
        ));

        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(ServersMetadata.TYPE),
            ServersMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(ForeignTablesMetadata.TYPE),
            parser -> ForeignTablesMetadata.fromXContent(nodeCtx, parser)
        ));

        return entries;
    }


    @Override
    protected void configure() {
    }
}
