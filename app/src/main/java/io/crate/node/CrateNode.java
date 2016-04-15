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

package io.crate.node;

import com.google.common.collect.ImmutableList;
import io.crate.plugin.CrateCorePlugin;
import io.crate.plugin.SrvPlugin;
import org.elasticsearch.Version;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugin.cloud.aws.CloudAwsPlugin;
import org.elasticsearch.plugin.discovery.multicast.MulticastDiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;

public class CrateNode extends Node {

    private static final Collection<Class<? extends Plugin>> CLASSPATH_PLUGINS = ImmutableList.of(
            CrateCorePlugin.class,
            MulticastDiscoveryPlugin.class,
            SrvPlugin.class,
            CloudAwsPlugin.class);

    public CrateNode(Environment environment) {
        super(environment, Version.CURRENT, CLASSPATH_PLUGINS);
    }
}
