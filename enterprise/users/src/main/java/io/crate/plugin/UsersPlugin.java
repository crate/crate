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

import io.crate.metadata.UsersMetaData;
import io.crate.metadata.UsersPrivilegesMetaData;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.List;

public class UsersPlugin extends Plugin {

    public UsersPlugin() {
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(4);
        entries.add(new NamedWriteableRegistry.Entry(
            MetaData.Custom.class,
            UsersMetaData.TYPE,
            UsersMetaData::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            UsersMetaData.TYPE,
            in -> UsersMetaData.readDiffFrom(MetaData.Custom.class, UsersMetaData.TYPE, in)
        ));

        entries.add(new NamedWriteableRegistry.Entry(
            MetaData.Custom.class,
            UsersPrivilegesMetaData.TYPE,
            UsersPrivilegesMetaData::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            UsersPrivilegesMetaData.TYPE,
            in -> UsersPrivilegesMetaData.readDiffFrom(MetaData.Custom.class, UsersPrivilegesMetaData.TYPE, in)
        ));
        return entries;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(2);
        entries.add(new NamedXContentRegistry.Entry(
            UsersMetaData.class,
            new ParseField(UsersMetaData.TYPE),
            UsersMetaData::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            UsersPrivilegesMetaData.class,
            new ParseField(UsersPrivilegesMetaData.TYPE),
            UsersPrivilegesMetaData::fromXContent
        ));
        return entries;
    }
}
