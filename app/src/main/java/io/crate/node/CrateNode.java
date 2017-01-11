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
import io.crate.plugin.*;
import io.crate.udc.plugin.UDCPlugin;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;

public class CrateNode extends Node {

    private static final Collection<Class<? extends Plugin>> CLASSPATH_PLUGINS = ImmutableList.of(
        PluginLoaderPlugin.class,
        CrateCorePlugin.class,
        BlobPlugin.class,
        // FIXME: MulticastDiscoveryPlugin is gone at ES
        //MulticastDiscoveryPlugin.class,
        SrvPlugin.class,
        UDCPlugin.class,
        // FIXME: cloud-aws plugin is split into discovery-ec2 and repository-s3
        //CloudAwsPlugin.class,
        AdminUIPlugin.class);

    public CrateNode(Environment environment) {
        super(environment, CLASSPATH_PLUGINS);
    }
}
