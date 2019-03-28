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

package org.elasticsearch;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class Version implements Comparable<Version>, ToXContentFragment {
    /*
     * The logic for ID is: XXYYZZAA, where XX is major version, YY is minor version, ZZ is revision, and AA is alpha/beta/rc indicator AA
     * values below 25 are for alpha builder (since 5.0), and above 25 and below 50 are beta builds, and below 99 are RC builds, with 99
     * indicating a release the (internal) format of the id is there so we can easily do after/before checks on the id
     */
    private static final int V_EMPTY_ID = 0;
    public static final Version V_EMPTY = new Version(V_EMPTY_ID, 0, org.apache.lucene.util.Version.LATEST);
    private static final int ES_V_6_1_4_ID = 6010499;
    public static final Version ES_V_6_1_4 = new Version(ES_V_6_1_4_ID, 3_00_01_99, org.apache.lucene.util.Version.LUCENE_7_1_0);
    private static final int ES_V_6_5_1_ID = 6050199;
    public static final Version ES_V_6_5_1 = new Version(ES_V_6_5_1_ID, 3_02_00_99, org.apache.lucene.util.Version.LUCENE_7_5_0);

    /**
     * Before CrateDB 4.0 we've had ES versions (internalId) and CrateDB (externalId) versions.
     * The internalId is stored in indices, so we keep using it for compatibility.
     *
     * Starting with CrateDB 4.0 we only have a single version, but keep maintaining an internalId for compatibility.
     * This is a static-offset that maps CrateDB (externalId) to internalId.
     *
     * E.g.
     *
     * CrateDB 4.0.0 -> 7.0.0
     *         4.0.1 -> 7.0.1
     *         4.1.3 -> 7.1.3
     *         5.0.3 -> 8.0.3
     *         ...
     */
    private static final int INTERNAL_OFFSET = 3_00_00_00;

    public static final int ES_V_7_0_0_ID = 7_00_00_99;
    public static final Version V_4_0_0 = new Version(ES_V_7_0_0_ID, ES_V_7_0_0_ID - INTERNAL_OFFSET, true, org.apache.lucene.util.Version.LUCENE_8_0_0);

    public static final Version CURRENT = V_4_0_0;


    static {
        assert CURRENT.luceneVersion.equals(org.apache.lucene.util.Version.LATEST) : "Version must be upgraded to ["
                + org.apache.lucene.util.Version.LATEST + "] is still set to [" + CURRENT.luceneVersion + "]";
    }

    public static Version readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static Version fromId(int internalId) {
        switch (internalId) {
            case ES_V_6_5_1_ID:
                return ES_V_6_5_1;
            case ES_V_6_1_4_ID:
                return ES_V_6_1_4;
            case ES_V_7_0_0_ID:
                return V_4_0_0;
            case V_EMPTY_ID:
                return V_EMPTY;
            default:
                throw new IllegalStateException("Illegal internal version id: " + internalId);
        }
    }

    /**
     * Return the {@link Version} of Elasticsearch that has been used to create an index given its settings.
     *
     * @throws IllegalStateException if the given index settings doesn't contain a value for the key
     *         {@value IndexMetaData#SETTING_VERSION_CREATED}
     */
    public static Version indexCreated(Settings indexSettings) {
        final Version indexVersion = IndexMetaData.SETTING_INDEX_VERSION_CREATED.get(indexSettings);
        if (indexVersion == V_EMPTY) {
            final String message = String.format(
                    Locale.ROOT,
                    "[%s] is not present in the index settings for index with UUID [%s]",
                    IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(),
                    indexSettings.get(IndexMetaData.SETTING_INDEX_UUID));
            throw new IllegalStateException(message);
        }
        return indexVersion;
    }

    public static void writeVersion(Version version, StreamOutput out) throws IOException {
        out.writeVInt(version.internalId);
    }

    /**
     * Returns the minimum version between the 2.
     */
    public static Version min(Version version1, Version version2) {
        return version1.internalId < version2.internalId ? version1 : version2;
    }

    /**
     * Returns the maximum version between the 2
     */
    public static Version max(Version version1, Version version2) { return version1.internalId > version2.internalId ? version1 : version2; }

    /**
     * Returns the version given its string representation, current version if the argument is null or empty
     */
    public static Version fromInternalString(String version) {
        if (!Strings.hasLength(version)) {
            return Version.CURRENT;
        }
        final boolean snapshot; // this is some BWC for 2.x and before indices
        if (snapshot = version.endsWith("-SNAPSHOT")) {
            version = version.substring(0, version.length() - 9);
        }
        String[] parts = version.split("[.-]");
        if (parts.length < 3 || parts.length > 4) {
            throw new IllegalArgumentException(
                    "the version needs to contain major, minor, and revision, and optionally the build: " + version);
        }

        try {
            final int rawMajor = Integer.parseInt(parts[0]);
            if (rawMajor >= 5 && snapshot) { // we don't support snapshot as part of the version here anymore
                throw new IllegalArgumentException("illegal version format - snapshots are only supported until version 2.x");
            }
            final int betaOffset = rawMajor < 5 ? 0 : 25;
            //we reverse the version id calculation based on some assumption as we can't reliably reverse the modulo
            final int major = rawMajor * 1000000;
            final int minor = Integer.parseInt(parts[1]) * 10000;
            final int revision = Integer.parseInt(parts[2]) * 100;


            int build = 99;
            if (parts.length == 4) {
                String buildStr = parts[3];
                if (buildStr.startsWith("alpha")) {
                    assert rawMajor >= 5 : "major must be >= 5 but was " + major;
                    build = Integer.parseInt(buildStr.substring(5));
                    assert build < 25 : "expected a beta build but " + build + " >= 25";
                } else if (buildStr.startsWith("Beta") || buildStr.startsWith("beta")) {
                    build = betaOffset + Integer.parseInt(buildStr.substring(4));
                    assert build < 50 : "expected a beta build but " + build + " >= 50";
                } else if (buildStr.startsWith("RC") || buildStr.startsWith("rc")) {
                    build = Integer.parseInt(buildStr.substring(2)) + 50;
                } else {
                    throw new IllegalArgumentException("unable to parse version " + version);
                }
            }

            return fromId(major + minor + revision + build);

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse version " + version, e);
        }
    }

    private final boolean isSnapshot;
    public final int externalId;
    public final int internalId;
    public final byte major;
    public final byte minor;
    public final byte revision;
    public final byte build;
    public final org.apache.lucene.util.Version luceneVersion;

    Version(int internalId, int externalId, org.apache.lucene.util.Version luceneVersion) {
        this(internalId, externalId, false, luceneVersion);
    }

    Version(int internalId, int externalId, boolean isSnapshot, org.apache.lucene.util.Version luceneVersion) {
        this.internalId = internalId;
        this.externalId = externalId;
        this.major = (byte) ((internalId / 1000000) % 100);
        this.minor = (byte) ((internalId / 10000) % 100);
        this.revision = (byte) ((internalId / 100) % 100);
        this.build = (byte) (internalId % 100);
        this.luceneVersion = luceneVersion;
        this.isSnapshot = isSnapshot;
    }

    public boolean isSnapshot() {
        return isSnapshot;
    }

    public boolean after(Version version) {
        return version.internalId < internalId;
    }

    public boolean onOrAfter(Version version) {
        return version.internalId <= internalId;
    }

    public boolean before(Version version) {
        return version.internalId > internalId;
    }

    public boolean onOrBefore(Version version) {
        return version.internalId >= internalId;
    }

    @Override
    public int compareTo(Version other) {
        return Integer.compare(this.internalId, other.internalId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(toString());
    }

    /**
     * Returns the minimum compatible version based on the current
     * version. Ie a node needs to have at least the return version in order
     * to communicate with a node running the current version. The returned version
     * is in most of the cases the smallest major version release unless the current version
     * is a beta or RC release then the version itself is returned.
     */
    public Version minimumCompatibilityVersion() {
        return ES_V_6_1_4;
    }

    /**
     * Returns the minimum created index version that this version supports. Indices created with lower versions
     * can't be used with this version. This should also be used for file based serialization backwards compatibility ie. on serialization
     * code that is used to read / write file formats like transaction logs, cluster state, and index metadata.
     */
    public Version minimumIndexCompatibilityVersion() {
        return ES_V_6_1_4;
    }

    /**
     * Returns <code>true</code> iff both version are compatible. Otherwise <code>false</code>
     */
    public boolean isCompatible(Version version) {
        boolean compatible = onOrAfter(version.minimumCompatibilityVersion())
            && version.onOrAfter(minimumCompatibilityVersion());

        assert compatible == false || Math.max(major, version.major) - Math.min(major, version.major) <= 1;
        return compatible;
    }

    @SuppressForbidden(reason = "System.out.*")
    public static void main(String[] args) {
        final String versionOutput = String.format(
                Locale.ROOT,
                "Version: %s, Build: %s/%s, JVM: %s",
                Version.displayVersion(Version.CURRENT, Version.CURRENT.isSnapshot()),
                Build.CURRENT.hashShort(),
                Build.CURRENT.timestamp(),
                JvmInfo.jvmInfo().version());
        System.out.println(versionOutput);
    }

    @Override
    public String toString() {
        return externalNumber();
    }

    public String externalNumber() {
        return Integer.toString((externalId / 1000_000) % 100)
               + '.'
               + (externalId / 10_000) % 100
               + '.'
               + (externalId / 100) % 100;
    }

    public static String displayVersion(final Version version, final boolean isSnapshot) {
        return version + (isSnapshot ? "-SNAPSHOT" : "");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Version version = (Version) o;

        if (internalId != version.internalId) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return internalId;
    }

    public boolean isRelease() {
        return build == 99;
    }

    /**
     * Extracts a sorted list of declared version constants from a class.
     * The argument would normally be Version.class but is exposed for
     * testing with other classes-containing-version-constants.
     */
    public static List<Version> getDeclaredVersions(final Class<?> versionClass) {
        final Field[] fields = versionClass.getFields();
        final List<Version> versions = new ArrayList<>(fields.length);
        for (final Field field : fields) {
            final int mod = field.getModifiers();
            if (false == Modifier.isStatic(mod) && Modifier.isFinal(mod) && Modifier.isPublic(mod)) {
                continue;
            }
            if (field.getType() != Version.class) {
                continue;
            }
            switch (field.getName()) {
                case "CURRENT":
                case "V_EMPTY":
                    continue;
            }
            assert field.getName().matches("(ES_)?V(_\\d+)+(_(alpha|beta|rc)\\d+)?") : field.getName();
            try {
                versions.add(((Version) field.get(null)));
            } catch (final IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        Collections.sort(versions);
        return versions;
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
