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
import io.crate.Build;
import io.crate.Version;
import io.crate.plugin.BlobPlugin;
import io.crate.plugin.CrateCorePlugin;
import io.crate.plugin.HttpTransportPlugin;
import io.crate.plugin.PluginLoaderPlugin;
import io.crate.plugin.SrvPlugin;
import io.crate.udc.plugin.UDCPlugin;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugin.discovery.ec2.Ec2DiscoveryPlugin;
import org.elasticsearch.plugin.repository.s3.S3RepositoryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;

import java.util.Arrays;
import java.util.Collection;

public class CrateNode extends Node {

    private static final Collection<Class<? extends Plugin>> CLASSPATH_PLUGINS = ImmutableList.of(
        PluginLoaderPlugin.class,
        CrateCorePlugin.class,
        HttpTransportPlugin.class,
        BlobPlugin.class,
        SrvPlugin.class,
        UDCPlugin.class,
        S3RepositoryPlugin.class,
        Ec2DiscoveryPlugin.class,
        Netty4Plugin.class);

    protected CrateNode(Environment environment) {
        super(environment, CLASSPATH_PLUGINS);
    }

    @Override
    protected void startUpLogging(Logger logger, Settings tmpSettings, boolean hadPredefinedNodeName) {
        final JvmInfo jvmInfo = JvmInfo.jvmInfo();
        logger.info(
            "CrateDB version[{}], pid[{}], build[{}/{}], OS[{}/{}/{}], JVM[{}/{}/{}/{}]",
            Version.CURRENT,
            jvmInfo.pid(),
            Build.CURRENT.hashShort(),
            Build.CURRENT.timestamp(),
            Constants.OS_NAME,
            Constants.OS_VERSION,
            Constants.OS_ARCH,
            Constants.JVM_VENDOR,
            Constants.JVM_NAME,
            Constants.JAVA_VERSION,
            Constants.JVM_VERSION);

        if (logger.isDebugEnabled()) {
            Environment environment = getEnvironment();
            logger.debug("using config [{}], data [{}], logs [{}], plugins [{}]",
                environment.configFile(), Arrays.toString(environment.dataFiles()), environment.logsFile(), environment.pluginsFile());
        }
    }
}
