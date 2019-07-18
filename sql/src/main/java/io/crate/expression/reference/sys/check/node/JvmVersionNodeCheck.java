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

package io.crate.expression.reference.sys.check.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.StringTokenizer;

@Singleton
public final class JvmVersionNodeCheck extends AbstractSysNodeCheck {

    public static final int ID = 8;
    private static final Logger LOGGER = LogManager.getLogger(JvmVersionNodeCheck.class);

    private static final String DESCRIPTION =
        "The JVM version with which CrateDB is running should be >= 11. " +
        "Support for older versions will be removed in the future. Current version is: " + Constants.JAVA_VERSION;

    @Inject
    public JvmVersionNodeCheck() {
        super(ID, DESCRIPTION, Severity.LOW);
    }

    @Override
    public boolean isValid() {
        try {
            return isOnOrAfter11(Constants.JAVA_VERSION);
        } catch (Exception e) {
            LOGGER.error("Error parsing java version={} error={}", Constants.JAVA_VERSION, e);
            return false;
        }
    }

    /**
     * Parse a Java version string into an array of [major, minor, hotfix]
     */
    static int[] parseVersion(String javaVersion) {
        StringTokenizer tokenizer = new StringTokenizer(javaVersion, ".-_ea");
        int major;
        if (tokenizer.hasMoreTokens()) {
            major = Integer.parseInt(tokenizer.nextToken());
        } else {
            throw new IllegalArgumentException("Cannot parse major version from java version: " + javaVersion);
        }
        int minor = 0;
        if (tokenizer.hasMoreTokens()) {
            try {
                minor = Integer.parseInt(tokenizer.nextToken());
            } catch (NumberFormatException ignored) {
            }
        }
        int patch = 0;
        if (tokenizer.hasMoreTokens()) {
            try {
                patch = Integer.parseInt(tokenizer.nextToken());
            } catch (NumberFormatException ignored) {
            }
        }
        return new int[] { major, minor, patch };
    }

    static boolean isOnOrAfter11(String javaVersion) {
        int[] version = parseVersion(javaVersion);
        return version[0] >= 11;
    }
}
