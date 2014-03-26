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
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.IOException;

public class Version {


    // The logic for ID is: XXYYZZAA, where XX is major version, YY is minor version, ZZ is revision, and AA is Beta/RC indicator
    // AA values below 50 are beta builds, and below 99 are RC builds, with 99 indicating a release
    // the (internal) format of the id is there so we can easily do after/before checks on the id

    public static final int V_0_20_00_ID = /*00*/200099;
    public static final Version V_0_20_00 = new Version(V_0_20_00_ID, false,
        org.elasticsearch.Version.V_0_90_7);

    public static final int V_0_20_01_ID = /*00*/200199;
    public static final Version V_0_20_01 = new Version(V_0_20_01_ID, false,
            org.elasticsearch.Version.V_0_90_7);

    public static final int V_0_20_02_ID = /*00*/200299;
    public static final Version V_0_20_02 = new Version(V_0_20_02_ID, false,
            org.elasticsearch.Version.V_0_90_7);

    public static final int V_0_20_03_ID = /*00*/200399;
    public static final Version V_0_20_03 = new Version(V_0_20_03_ID, false,
            org.elasticsearch.Version.V_0_90_7);

    public static final int V_0_20_04_ID = /*00*/200499;
    public static final Version V_0_20_04 = new Version(V_0_20_04_ID, false,
            org.elasticsearch.Version.V_0_90_7);

    public static final int V_0_21_00_ID = /*00*/210099;
    public static final Version V_0_21_00 = new Version(V_0_21_00_ID, false,
            org.elasticsearch.Version.V_0_90_7);

    public static final int V_0_21_01_ID = /*00*/210199;
    public static final Version V_0_21_01 = new Version(V_0_21_01_ID, false,
            org.elasticsearch.Version.V_0_90_7);

    public static final int V_0_22_00_ID = /*00*/220099;
    public static final Version V_0_22_00 = new Version(V_0_22_00_ID, false,
            org.elasticsearch.Version.V_0_90_11);

    public static final int V_0_22_01_ID = /*00*/220199;
    public static final Version V_0_22_01 = new Version(V_0_22_01_ID, false,
            org.elasticsearch.Version.V_0_90_11);

    public static final int V_0_22_02_ID = /*00*/220299;
    public static final Version V_0_22_02 = new Version(V_0_22_02_ID, false,
            org.elasticsearch.Version.V_0_90_11);

    public static final int V_0_23_00_ID = /*00*/230000;
    public static final Version V_0_23_00 = new Version(V_0_23_00_ID, false,
            org.elasticsearch.Version.V_0_90_11);

    public static final int V_0_23_01_ID = /*00*/230199;
    public static final Version V_0_23_01 = new Version(V_0_23_01_ID, false,
            org.elasticsearch.Version.V_0_90_11);

    public static final int V_0_23_02_ID = /*00*/230299;
    public static final Version V_0_23_02 = new Version(V_0_23_02_ID, false,
            org.elasticsearch.Version.V_0_90_11);

    public static final int V_0_24_00_ID = /*00*/240099;
    public static final Version V_0_24_00 = new Version(V_0_24_00_ID, false,
            org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_25_00_ID = /*00*/250099;
    public static final Version V_0_25_00 = new Version(V_0_25_00_ID, false,
        org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_26_00_ID = /*00*/260099;
    public static final Version V_0_26_00 = new Version(V_0_26_00_ID, false,
        org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_27_00_ID = /*00*/270099;
    public static final Version V_0_27_00 = new Version(V_0_27_00_ID, false,
            org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_28_00_ID = /*00*/280099;
    public static final Version V_0_28_00 = new Version(V_0_28_00_ID, false,
            org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_29_00_ID = /*00*/290099;
    public static final Version V_0_29_00 = new Version(V_0_29_00_ID, false,
            org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_30_00_ID = /*00*/300099;
    public static final Version V_0_30_00 = new Version(V_0_30_00_ID, false,
            org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_31_00_ID = /*00*/310099;
    public static final Version V_0_31_00 = new Version(V_0_31_00_ID, false,
            org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_32_00_ID = /*00*/320099;
    public static final Version V_0_32_00 = new Version(V_0_32_00_ID, false,
            org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_32_01_ID = /*00*/320199;
    public static final Version V_0_32_01 = new Version(V_0_32_01_ID, false,
            org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_32_02_ID = /*00*/320299;
    public static final Version V_0_32_02 = new Version(V_0_32_02_ID, false,
            org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_32_03_ID = /*00*/320399;
    public static final Version V_0_32_03 = new Version(V_0_32_03_ID, false,
            org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_33_00_ID = /*00*/330099;
    public static final Version V_0_33_00 = new Version(V_0_33_00_ID, false,
            org.elasticsearch.Version.V_1_0_1);

    public static final int V_0_34_00_ID = /*00*/340099;
    public static final Version V_0_34_00 = new Version(V_0_34_00_ID, false,
            org.elasticsearch.Version.V_1_0_1);

    public static final Version CURRENT = V_0_34_00;

    static {
        assert CURRENT.esVersion == org.elasticsearch.Version.CURRENT : "Version must be " +
                "upgraded to [" + org.elasticsearch.Version.CURRENT + "] is still set to [" +
                CURRENT.esVersion + "]";
    }

    public static Version readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static Version fromId(int id) {
        switch (id) {
            case V_0_20_00_ID:
                return V_0_20_00;
            case V_0_20_01_ID:
                return V_0_20_01;
            case V_0_20_02_ID:
                return V_0_20_02;
            case V_0_20_03_ID:
                return V_0_20_03;
            case V_0_20_04_ID:
                return V_0_20_04;
            case V_0_21_00_ID:
                return V_0_21_00;
            case V_0_21_01_ID:
                return V_0_21_01;
            case V_0_22_00_ID:
                return V_0_22_00;
            case V_0_22_01_ID:
                return V_0_22_01;
            case V_0_22_02_ID:
                return V_0_22_02;
            case V_0_23_00_ID:
                return V_0_23_00;
            case V_0_23_01_ID:
                return V_0_23_01;
            case V_0_23_02_ID:
                return V_0_23_02;
            case V_0_24_00_ID:
                return V_0_24_00;
            case V_0_25_00_ID:
                return V_0_25_00;
            case V_0_26_00_ID:
                return V_0_26_00;
            case V_0_27_00_ID:
                return V_0_27_00;
            case V_0_28_00_ID:
                return V_0_28_00;
            case V_0_29_00_ID:
                return V_0_29_00;
            case V_0_30_00_ID:
                return V_0_30_00;
            case V_0_31_00_ID:
                return V_0_31_00;
            case V_0_32_00_ID:
                return V_0_32_00;
            case V_0_32_01_ID:
                return V_0_32_01;
            case V_0_32_02_ID:
                return V_0_32_02;
            case V_0_32_03_ID:
                return V_0_32_03;
            case V_0_33_00_ID:
                return V_0_33_00;
            case V_0_34_00_ID:
                return V_0_34_00;
            default:
                return new Version(id, null, org.elasticsearch.Version.CURRENT);
        }
    }

    public static void writeVersion(Version version, StreamOutput out) throws IOException {
        out.writeVInt(version.id);
    }

    /**
     * Returns the smallest version between the 2.
     */
    public static Version smallest(Version version1, Version version2) {
        return version1.id < version2.id ? version1 : version2;
    }

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

    public boolean after(Version version) {
        return version.id < id;
    }

    public boolean onOrAfter(Version version) {
        return version.id <= id;
    }

    public boolean before(Version version) {
        return version.id > id;
    }

    public boolean onOrBefore(Version version) {
        return version.id >= id;
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
                ", JVM: " + JvmInfo.jvmInfo().version() );
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

    public static class Module extends AbstractModule {

        private final Version version;

        public Module(Version version) {
            this.version = version;
        }

        @Override
        protected void configure() {
            bind(Version.class).toInstance(version);
        }
    }
}
