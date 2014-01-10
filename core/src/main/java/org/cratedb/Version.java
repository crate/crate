package org.cratedb;


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

    public static final int V_0_19_5_ID = /*00*/190599;
    public static final Version V_0_19_5 = new Version(V_0_19_5_ID, false,
            org.elasticsearch.Version.V_0_90_5);
    public static final int V_0_19_6_ID = /*00*/190699;
    public static final Version V_0_19_6 = new Version(V_0_19_6_ID, false,
            org.elasticsearch.Version.V_0_90_5);
    public static final int V_0_19_7_ID = /*00*/190799;
    public static final Version V_0_19_7 = new Version(V_0_19_7_ID, false,
            org.elasticsearch.Version.V_0_90_5);
    public static final int V_0_19_8_ID = /*00*/190899;
    public static final Version V_0_19_8 = new Version(V_0_19_8_ID, false,
            org.elasticsearch.Version.V_0_90_5);
    public static final int V_0_19_9_ID = /*00*/190999;
    public static final Version V_0_19_9 = new Version(V_0_19_9_ID, false,
        org.elasticsearch.Version.V_0_90_7);
    public static final int V_0_19_10_ID = /*00*/191099;
    public static final Version V_0_19_10 = new Version(V_0_19_10_ID, false,
        org.elasticsearch.Version.V_0_90_7);

    public static final int V_0_19_11_ID = /*00*/191199;
    public static final Version V_0_19_11 = new Version(V_0_19_11_ID, false,
        org.elasticsearch.Version.V_0_90_7);

    public static final int V_0_19_12_ID = /*00*/191299;
    public static final Version V_0_19_12 = new Version(V_0_19_12_ID, false,
        org.elasticsearch.Version.V_0_90_7);

    public static final int V_0_19_13_ID = /*00*/191399;
    public static final Version V_0_19_13 = new Version(V_0_19_13_ID, false,
            org.elasticsearch.Version.V_0_90_7);

    public static final int V_0_19_14_ID = /*00*/191499;
    public static final Version V_0_19_14 = new Version(V_0_19_14_ID, false,
            org.elasticsearch.Version.V_0_90_7);

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

    public static final Version CURRENT = V_0_20_04;

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
            case V_0_19_5_ID:
                return V_0_19_5;
            case V_0_19_6_ID:
                return V_0_19_6;
            case V_0_19_7_ID:
                return V_0_19_7;
            case V_0_19_8_ID:
                return V_0_19_8;
            case V_0_19_9_ID:
                return V_0_19_9;
            case V_0_19_10_ID:
                return V_0_19_10;
            case V_0_19_11_ID:
                return V_0_19_11;
            case V_0_19_12_ID:
                return V_0_19_12;
            case V_0_19_13_ID:
                return V_0_19_13;
            case V_0_19_14_ID:
                return V_0_19_14;
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
