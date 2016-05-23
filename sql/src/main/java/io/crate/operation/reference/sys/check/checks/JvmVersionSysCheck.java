/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.reference.sys.check.checks;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.inject.Singleton;

import java.util.StringTokenizer;

@Singleton
public class JvmVersionSysCheck extends AbstractSysCheck {

    private static final int ID = 6;
    private static final int MIN_UPDATE_VERSION = 20;
    private static final int MIN_MAJOR_VERSION = 8;
    private static final String DESCRIPTION = "Crate is running with " + Constants.JAVA_VERSION +
            " java version. Java 7 is deprecated since version 0.55.0. Please update to Java 8 (>= update 20) runtime environment.";

    public JvmVersionSysCheck() {
        super(ID, DESCRIPTION, Severity.MEDIUM);
    }

    @Override
    public boolean validate() {
        return validateJavaVersion(Constants.JAVA_VERSION);
    }

    protected boolean validateJavaVersion(String javaVersion) {
        int javaUpdate;
        int javaMajorVersion;

        try {
            final StringTokenizer st = new StringTokenizer(javaVersion, "_");
            String javaSpecVersion = st.nextToken();

            final StringTokenizer st2 = new StringTokenizer(st.nextToken(), "-");
            javaUpdate = Integer.parseInt(st2.nextToken());

            final StringTokenizer st3 = new StringTokenizer(javaSpecVersion, ".");
            st3.nextToken();
            javaMajorVersion = Integer.parseInt(st3.nextToken());
        } catch (Exception ex) {
            return false;
        }
        return (javaMajorVersion >= MIN_MAJOR_VERSION) && (javaUpdate >= MIN_UPDATE_VERSION);
    }


}
