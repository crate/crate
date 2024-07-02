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

package org.elasticsearch.index.seqno;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;

public class RetentionLeasesTests extends ESTestCase {

    public void testPrimaryTermOutOfRange() {
        final long primaryTerm = randomLongBetween(Long.MIN_VALUE, 0);
        assertThatThrownBy(() -> new RetentionLeases(primaryTerm, randomNonNegativeLong(), Collections.emptyList()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("primary term must be positive but was [" + primaryTerm + "]");
    }

    public void testVersionOutOfRange() {
        final long version = randomLongBetween(Long.MIN_VALUE, -1);
        assertThatThrownBy(() -> new RetentionLeases(randomLongBetween(1, Long.MAX_VALUE), version, Collections.emptyList()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("version must be non-negative but was [" + version + "]");
    }

    public void testSupersedesByPrimaryTerm() {
        final long lowerPrimaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final RetentionLeases left = new RetentionLeases(lowerPrimaryTerm, randomLongBetween(1, Long.MAX_VALUE), Collections.emptyList());
        final long higherPrimaryTerm = randomLongBetween(lowerPrimaryTerm + 1, Long.MAX_VALUE);
        final RetentionLeases right = new RetentionLeases(higherPrimaryTerm, randomLongBetween(1, Long.MAX_VALUE), Collections.emptyList());
        assertThat(right.supersedes(left)).isTrue();
        assertThat(right.supersedes(left.primaryTerm(), left.version())).isTrue();
        assertThat(left.supersedes(right)).isFalse();
        assertThat(left.supersedes(right.primaryTerm(), right.version())).isFalse();
    }

    public void testSupersedesByVersion() {
        final long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final long lowerVersion = randomLongBetween(1, Long.MAX_VALUE);
        final long higherVersion = randomLongBetween(lowerVersion + 1, Long.MAX_VALUE);
        final RetentionLeases left = new RetentionLeases(primaryTerm, lowerVersion, Collections.emptyList());
        final RetentionLeases right = new RetentionLeases(primaryTerm, higherVersion, Collections.emptyList());
        assertThat(right.supersedes(left)).isTrue();
        assertThat(right.supersedes(left.primaryTerm(), left.version())).isTrue();
        assertThat(left.supersedes(right)).isFalse();
        assertThat(left.supersedes(right.primaryTerm(), right.version())).isFalse();
    }

    public void testRetentionLeasesRejectsDuplicates() {
        final RetentionLeases retentionLeases = randomRetentionLeases(false);
        final RetentionLease retentionLease = randomFrom(retentionLeases.leases());
        assertThatThrownBy(() -> new RetentionLeases(
                        retentionLeases.primaryTerm(),
                        retentionLeases.version(),
                        Stream.concat(retentionLeases.leases().stream(), Stream.of(retentionLease)).collect(Collectors.toList()))
        ).isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("duplicate retention lease ID [" + retentionLease.id() + "]");
    }

    public void testLeasesPreservesIterationOrder() {
        final RetentionLeases retentionLeases = randomRetentionLeases(true);
        if (retentionLeases.leases().isEmpty()) {
            assertThat(retentionLeases.leases()).isEmpty();
        } else {
            assertThat(retentionLeases.leases()).containsExactly(retentionLeases.leases().toArray(new RetentionLease[0]));
        }
    }

    public void testRetentionLeasesMetadataStateFormat() throws IOException {
        final Path path = createTempDir();
        final RetentionLeases retentionLeases = randomRetentionLeases(true);
        RetentionLeases.FORMAT.writeAndCleanup(retentionLeases, path);
        assertThat(RetentionLeases.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, path)).isEqualTo(retentionLeases);
    }

    private RetentionLeases randomRetentionLeases(boolean allowEmpty) {
        final long primaryTerm = randomNonNegativeLong();
        final long version = randomNonNegativeLong();
        final int length = randomIntBetween(allowEmpty ? 0 : 1, 8);
        final List<RetentionLease> leases = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            final String id = randomAlphaOfLength(8);
            final long retainingSequenceNumber = randomNonNegativeLong();
            final long timestamp = randomNonNegativeLong();
            final String source = randomAlphaOfLength(8);
            final RetentionLease retentionLease = new RetentionLease(id, retainingSequenceNumber, timestamp, source);
            leases.add(retentionLease);
        }
        return new RetentionLeases(primaryTerm, version, leases);
    }

}
