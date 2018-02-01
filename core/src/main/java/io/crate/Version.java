/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate;


import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public class Version {


    // The logic for ID is: XXYYZZAA, where XX is major version, YY is minor version, ZZ is revision, and AA is Beta/RC indicator
    // AA values below 50 are beta builds, and below 99 are RC builds, with 99 indicating a release
    // the (internal) format of the id is there so we can easily do after/before checks on the id


    public static final boolean SNAPSHOT = true;
    public static final Version CURRENT = new Version(3000099, SNAPSHOT, org.elasticsearch.Version.CURRENT);

    static {
        // safe-guard that we don't release a version with DEBUG_MODE set to true
        assert CURRENT.esVersion == org.elasticsearch.Version.CURRENT : "Version must be " +
                                                                        "upgraded to [" +
                                                                        org.elasticsearch.Version.CURRENT +
                                                                        "] is still set to [" +
                                                                        CURRENT.esVersion + "]";
    }

    public static final String CRATEDB_VERSION_KEY = "cratedb";
    public static final String ES_VERSION_KEY = "elasticsearch";

    public final int id;
    public final byte major;
    public final byte minor;
    public final byte revision;
    public final byte build;
    public final Boolean snapshot;
    public final org.elasticsearch.Version esVersion;

    Version(int id, @Nullable Boolean snapshot, org.elasticsearch.Version esVersion) {
        this.id = id;
        this.major = (byte) ((id / 1000000) % 100);
        this.minor = (byte) ((id / 10000) % 100);
        this.revision = (byte) ((id / 100) % 100);
        this.build = (byte) (id % 100);
        this.snapshot = snapshot;
        this.esVersion = esVersion;
    }

    public boolean snapshot() {
        return snapshot != null && snapshot;
    }

    /**
     * Just the version number (without -SNAPSHOT if snapshot).
     */
    public String number() {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(revision);
        if (build < 50) {
            sb.append(".Beta").append(build);
        } else if (build < 99) {
            sb.append(".RC").append(build - 50);
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println("Version: " + Version.CURRENT + ", Build: " +
                           Build.CURRENT.hashShort() + "/" + Build.CURRENT.timestamp() +
                           ", ES: " + org.elasticsearch.Version.CURRENT +
                           ", JVM: " + JvmInfo.jvmInfo().version());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(number());
        if (snapshot()) {
            sb.append("-SNAPSHOT");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Version version = (Version) o;

        if (id != version.id) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }

    public static Version readVersion(StreamInput in) throws IOException {
        return new Version(in.readVInt(),
            in.readBoolean(),
            org.elasticsearch.Version.readVersion(in));
    }

    public static void writeVersionTo(Version version, StreamOutput out) throws IOException {
        out.writeVInt(version.id);
        out.writeBoolean(version.snapshot);
        org.elasticsearch.Version.writeVersion(version.esVersion, out);
    }

    public static Map<String, Integer> toMap(Version version) {
        return MapBuilder.<String, Integer>newMapBuilder()
            .put(CRATEDB_VERSION_KEY, version.id)
            .put(ES_VERSION_KEY, version.esVersion.id)
            .map();
    }

    @Nullable
    public static Version fromMap(@Nullable Map<String, Integer> versionMap) {
        if (versionMap == null || versionMap.isEmpty()) {
            return null;
        }
        return new Version(
            versionMap.get(CRATEDB_VERSION_KEY),
            null, // snapshot info is not saved
            org.elasticsearch.Version.fromId(versionMap.get(ES_VERSION_KEY)));
    }

    public static Map<String, String> toStringMap(Version version) {
        return MapBuilder.<String, String>newMapBuilder()
            .put(CRATEDB_VERSION_KEY, version.number())
            .put(ES_VERSION_KEY, version.esVersion.toString())
            .map();
    }

    public enum Property {
        CREATED,
        UPGRADED;

        private String nameLowerCase;

        Property() {
            this.nameLowerCase = name().toLowerCase(Locale.ENGLISH);
        }

        @Override
        public String toString() {
            return nameLowerCase;
        }
    }
}
