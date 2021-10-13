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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hadoop.security.UserGroupInformation;
import org.elasticsearch.env.Environment;

/**
 * Oversees all the security specific logic for the HDFS Repository plugin.
 *
 * Keeps track of the current user for a given repository, as well as which
 * permissions to grant the blob store restricted execution methods.
 */
class HdfsSecurityContext {


    /**
     * Locates the keytab file in the environment and verifies that it exists.
     * Expects keytab file to exist at {@code $CONFIG_DIR$/repository-hdfs/krb5.keytab}
     */
    static Path locateKeytabFile(Environment environment) {
        Path keytabPath = environment.configFile().resolve("repository-hdfs").resolve("krb5.keytab");
        try {
            if (Files.exists(keytabPath) == false) {
                throw new RuntimeException("Could not locate keytab at [" + keytabPath + "].");
            }
        } catch (SecurityException se) {
            throw new RuntimeException("Could not locate keytab at [" + keytabPath + "]", se);
        }
        return keytabPath;
    }

    private final UserGroupInformation ugi;

    HdfsSecurityContext(UserGroupInformation ugi) {
        this.ugi = ugi;
    }

    void ensureLogin() {
        if (ugi.isFromKeytab()) {
            try {
                ugi.checkTGTAndReloginFromKeytab();
            } catch (IOException ioe) {
                throw new UncheckedIOException("Could not re-authenticate", ioe);
            }
        }
    }
}
