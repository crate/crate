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
package org.elasticsearch.test.hamcrest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.NotEqualMessageBuilder;

public class ElasticsearchAssertions {

    public static void assertNoTimeout(ClusterHealthResponse response) {
        assertThat(response.isTimedOut())
          .as("ClusterHealthResponse has timed out - returned: [" + response + "]")
          .isFalse();
    }

    public static void assertAcked(AcknowledgedResponse response) {
        assertThat(response.isAcknowledged()).as(response.getClass().getSimpleName() + " failed - not acked").isEqualTo(true);
    }

    /**
     * Assert that an index creation was fully acknowledged, meaning that both the index creation cluster
     * state update was successful and that the requisite number of shard copies were started before returning.
     */
    public static void assertAcked(CreateIndexResponse response) {
        assertThat(response.isAcknowledged()).as(response.getClass().getSimpleName() + " failed - not acked").isEqualTo(true);
        assertThat(response.isShardsAcknowledged()).as(response.getClass().getSimpleName() + " failed - index creation acked but not all shards were started").isTrue();
    }


    public static void assertNoFailures(BroadcastResponse response) {
        assertThat(response.getFailedShards()).as("Unexpected ShardFailures: " + Arrays.toString(response.getShardFailures())).isEqualTo(0);
    }

    /**
     * Asserts that the provided {@link BytesReference}s created through
     * {@link org.elasticsearch.common.xcontent.ToXContent#toXContent(XContentBuilder, ToXContent.Params)} hold the same content.
     * The comparison is done by parsing both into a map and comparing those two, so that keys ordering doesn't matter.
     * Also binary values (byte[]) are properly compared through arrays comparisons.
     */
    public static void assertToXContentEquivalent(BytesReference expected, BytesReference actual, XContentType xContentType)
            throws IOException {
        //we tried comparing byte per byte, but that didn't fly for a couple of reasons:
        //1) whenever anything goes through a map while parsing, ordering is not preserved, which is perfectly ok
        //2) Jackson SMILE parser parses floats as double, which then get printed out as double (with double precision)
        //Note that byte[] holding binary values need special treatment as they need to be properly compared item per item.
        Map<String, Object> actualMap = null;
        Map<String, Object> expectedMap = null;
        try (XContentParser actualParser = xContentType.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, actual.streamInput())) {
            actualMap = actualParser.map();
            try (XContentParser expectedParser = xContentType.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, expected.streamInput())) {
                expectedMap = expectedParser.map();
                try {
                    assertMapEquals(expectedMap, actualMap);
                } catch (AssertionError error) {
                    NotEqualMessageBuilder message = new NotEqualMessageBuilder();
                    message.compareMaps(actualMap, expectedMap);
                    throw new AssertionError("Error when comparing xContent.\n" + message.toString(), error);
                }
            }
        }
    }

    /**
     * Compares two maps recursively, using arrays comparisons for byte[] through Arrays.equals(byte[], byte[])
     */
    private static void assertMapEquals(Map<String, Object> expected, Map<String, Object> actual) {
        assertEquals(expected.size(), actual.size());
        for (Map.Entry<String, Object> expectedEntry : expected.entrySet()) {
            String expectedKey = expectedEntry.getKey();
            Object expectedValue = expectedEntry.getValue();
            if (expectedValue == null) {
                assertThat(actual.get(expectedKey) == null && actual.containsKey(expectedKey)).isTrue();
            } else {
                Object actualValue = actual.get(expectedKey);
                assertObjectEquals(expectedValue, actualValue);
            }
        }
    }

    /**
     * Compares two lists recursively, but using arrays comparisons for byte[] through Arrays.equals(byte[], byte[])
     */
    private static void assertListEquals(List<Object> expected, List<Object> actual) {
        assertEquals(expected.size(), actual.size());
        Iterator<Object> actualIterator = actual.iterator();
        for (Object expectedValue : expected) {
            Object actualValue = actualIterator.next();
            assertObjectEquals(expectedValue, actualValue);
        }
    }

    /**
     * Compares two objects, recursively walking eventual maps and lists encountered, and using arrays comparisons
     * for byte[] through Arrays.equals(byte[], byte[])
     */
    @SuppressWarnings("unchecked")
    private static void assertObjectEquals(Object expected, Object actual) {
        if (expected instanceof Map) {
            assertThat(actual).isInstanceOf(Map.class);
            assertMapEquals((Map<String, Object>) expected, (Map<String, Object>) actual);
        } else if (expected instanceof List) {
            assertListEquals((List<Object>) expected, (List<Object>) actual);
        } else if (expected instanceof byte[]) {
            //byte[] is really a special case for binary values when comparing SMILE and CBOR, arrays of other types
            //don't need to be handled. Ordinary arrays get parsed as lists.
            assertArrayEquals((byte[]) expected, (byte[]) actual);
        } else {
            assertEquals(expected, actual);
        }
    }
}
