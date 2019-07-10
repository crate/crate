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

package org.elasticsearch;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class VersionTest {

    @Test
    public void test_compatible_current_version_is_compatible_to_4_0_0() {
        assertThat(Version.CURRENT.isCompatible(Version.V_4_0_0), is(true));
    }

    @Test
    public void test_min_version_is_4_0_0() {
        assertThat(Version.CURRENT.minimumCompatibilityVersion(), is(Version.V_4_0_0));
    }

    @Test
    public void test_can_parse_next_patch_release_version() {
        int internalId = Version.CURRENT.internalId + 100;
        Version futureVersion = Version.fromId(internalId);
        assertThat(futureVersion.major, is(Version.CURRENT.major));
        assertThat(futureVersion.minor, is(Version.CURRENT.minor));
        assertThat(futureVersion.revision, is((byte) (Version.CURRENT.revision + 1)));
        assertThat(futureVersion.luceneVersion, is(Version.CURRENT.luceneVersion));
    }

    @Test
    public void test_can_parse_next_minor_release_version() {
        int internalId = Version.CURRENT.internalId + 10000;
        Version futureVersion = Version.fromId(internalId);
        assertThat(futureVersion.major, is(Version.CURRENT.major));
        assertThat(futureVersion.minor, is((byte) (Version.CURRENT.minor + 1)));
        assertThat(futureVersion.revision, is(Version.CURRENT.revision));
        assertThat(futureVersion.luceneVersion, is(Version.CURRENT.luceneVersion));
    }

    @Test
    public void test_can_parse_next_major_release_version() {
        int internalId = Version.CURRENT.internalId + 1000000;
        Version futureVersion = Version.fromId(internalId);
        assertThat(futureVersion.major, is((byte) (Version.CURRENT.major + 1)));
        assertThat(futureVersion.minor, is(Version.CURRENT.minor));
        assertThat(futureVersion.revision, is(Version.CURRENT.revision));
        assertThat(futureVersion.luceneVersion, is(org.apache.lucene.util.Version.fromBits(
            Version.CURRENT.luceneVersion.major + 1, 0, 0)
        ));
    }
}
