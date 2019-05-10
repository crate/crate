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
import io.crate.plugin.BlobPlugin;
import io.crate.plugin.CrateCommonPlugin;
import io.crate.plugin.HttpTransportPlugin;
import io.crate.plugin.PluginLoaderPlugin;
import io.crate.plugin.SrvPlugin;
import io.crate.udc.plugin.UDCPlugin;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.discovery.ec2.Ec2DiscoveryPlugin;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugin.repository.url.URLRepositoryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;
import org.elasticsearch.transport.Netty4Plugin;

import java.util.Collection;

public class CrateNode extends Node {

    private static final Collection<Class<? extends Plugin>> CLASSPATH_PLUGINS = ImmutableList.of(
        PluginLoaderPlugin.class,
        CrateCommonPlugin.class,
        HttpTransportPlugin.class,
        BlobPlugin.class,
        SrvPlugin.class,
        UDCPlugin.class,
        URLRepositoryPlugin.class,
        S3RepositoryPlugin.class,
        Ec2DiscoveryPlugin.class,
        CommonAnalysisPlugin.class,
        Netty4Plugin.class);

    protected CrateNode(Environment environment) {
        super(environment, CLASSPATH_PLUGINS, true);
    }
}

