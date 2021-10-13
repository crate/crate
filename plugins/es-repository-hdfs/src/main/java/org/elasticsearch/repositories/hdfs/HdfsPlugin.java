/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.hdfs;

import io.crate.analyze.repositories.TypeSettings;
import io.crate.common.SuppressForbidden;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;

import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

public final class HdfsPlugin extends Plugin implements RepositoryPlugin {

    // initialize some problematic classes
    static {
        HdfsPlugin.evilHadoopInit();
        HdfsPlugin.eagerInit();
    }

    @SuppressForbidden(reason = "Needs a security hack for hadoop on windows, until HADOOP-XXXX is fixed")
    private static Void evilHadoopInit() {
        // hack: on Windows, Shell's clinit has a similar problem that on unix,
        // but here we can workaround it for now by setting hadoop home
        // on unix: we still want to set this to something we control, because
        // if the user happens to have HADOOP_HOME in their environment -> checkHadoopHome goes boom
        // TODO: remove THIS when hadoop is fixed
        Path hadoopHome = null;
        String oldValue = null;
        try {
            hadoopHome = Files.createTempDirectory("hadoop").toAbsolutePath();
            oldValue = System.setProperty("hadoop.home.dir", hadoopHome.toString());
            Class.forName("org.apache.hadoop.security.UserGroupInformation");
            Class.forName("org.apache.hadoop.util.StringUtils");
            Class.forName("org.apache.hadoop.util.ShutdownHookManager");
            Class.forName("org.apache.hadoop.conf.Configuration");
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            // try to clean up the hack
            if (oldValue == null) {
                System.clearProperty("hadoop.home.dir");
            } else {
                System.setProperty("hadoop.home.dir", oldValue);
            }
            try {
                // try to clean up our temp dir too if we can
                if (hadoopHome != null) {
                    Files.delete(hadoopHome);
                }
            } catch (IOException thisIsBestEffort) {
                // ignored
            }
        }
        return null;
    }

    private static Void eagerInit() {
        /*
         * Hadoop RPC wire serialization uses ProtocolBuffers. All proto classes for Hadoop
         * come annotated with configurations that denote information about if they support
         * certain security options like Kerberos, and how to send information with the
         * message to support that authentication method. SecurityUtil creates a service loader
         * in a static field during its clinit. This loader provides the implementations that
         * pull the security information for each proto class. The service loader sources its
         * services from the current thread's context class loader, which must contain the Hadoop
         * jars. Since plugins don't execute with their class loaders installed as the thread's
         * context class loader, we need to install the loader briefly, allow the util to be
         * initialized, then restore the old loader since we don't actually own this thread.
         */
        ClassLoader oldCCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(HdfsRepository.class.getClassLoader());
            KerberosInfo info = SecurityUtil.getKerberosInfo(ClientNamenodeProtocolPB.class, null);
            // Make sure that the correct class loader was installed.
            if (info == null) {
                throw new RuntimeException("Could not initialize SecurityUtil: " +
                    "Unable to find services for [org.apache.hadoop.security.SecurityInfo]");
            }
        } finally {
            Thread.currentThread().setContextClassLoader(oldCCL);
        }
        return null;
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env,
                                                           NamedXContentRegistry namedXContentRegistry,
                                                           ClusterService clusterService) {
        return Collections.singletonMap(
            "hdfs",
            new Repository.Factory() {

                @Override
                public TypeSettings settings() {
                    Map<String, Setting<?>> optionalSettings = Map.ofEntries(
                        Map.entry("uri", Setting.simpleString("uri", Setting.Property.NodeScope)),
                        Map.entry("security.principal", Setting.simpleString("security.principal", Setting.Property.NodeScope)),
                        Map.entry("path", Setting.simpleString("path", Setting.Property.NodeScope)),
                        Map.entry("load_defaults", Setting.boolSetting("load_defaults", true, Setting.Property.NodeScope)),
                        Map.entry("compress", Setting.boolSetting("compress", true, Setting.Property.NodeScope)),
                        // We cannot use a ByteSize setting as it doesn't support NULL and it must be NULL as default to indicate to
                        // not override the default behaviour.
                        Map.entry("chunk_size", Setting.simpleString("chunk_size"))
                    );
                    return new TypeSettings(Map.of(), optionalSettings) {

                        @Override
                        public GenericProperties<?> dynamicProperties(GenericProperties<?> genericProperties) {
                            if (genericProperties.isEmpty()) {
                                return genericProperties;
                            }
                            GenericProperties<?> dynamicProperties = new GenericProperties<>();
                            for (Map.Entry<String, ?> entry : genericProperties.properties().entrySet()) {
                                String key = entry.getKey();
                                if (key.startsWith("conf.")) {
                                    dynamicProperties.add(new GenericProperty(key, entry.getValue()));
                                }
                            }
                            return dynamicProperties;
                        }
                    };
                }

                @Override
                public Repository create(RepositoryMetadata metadata) throws Exception {
                    return new HdfsRepository(metadata, env, namedXContentRegistry, clusterService);
                }
            }
        );
    }
}
