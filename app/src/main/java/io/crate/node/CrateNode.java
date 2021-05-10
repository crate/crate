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

package io.crate.node;

import java.util.Collection;
import java.util.List;

import org.elasticsearch.discovery.ec2.Ec2DiscoveryPlugin;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugin.repository.url.URLRepositoryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;
import org.elasticsearch.transport.Netty4Plugin;

import io.crate.plugin.BlobPlugin;
import io.crate.plugin.SQLPlugin;
import io.crate.plugin.SrvPlugin;
import io.crate.udc.plugin.UDCPlugin;

public class CrateNode extends Node {

    private static final Collection<Class<? extends Plugin>> CLASSPATH_PLUGINS = List.of(
        SQLPlugin.class,
        BlobPlugin.class,
        SrvPlugin.class,
        UDCPlugin.class,
        URLRepositoryPlugin.class,
        S3RepositoryPlugin.class,
        Ec2DiscoveryPlugin.class,
        Netty4Plugin.class);

    protected CrateNode(Environment environment) {
        super(environment, CLASSPATH_PLUGINS, true);
    }
}

