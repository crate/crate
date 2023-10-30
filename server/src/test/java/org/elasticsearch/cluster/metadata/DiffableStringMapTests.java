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

package org.elasticsearch.cluster.metadata;

import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

public class DiffableStringMapTests extends ESTestCase {

    public void testDiffableStringMapDiff() {
        Map<String, String> m = new HashMap<>();
        m.put("foo", "bar");
        m.put("baz", "eggplant");
        m.put("potato", "canon");
        DiffableStringMap dsm = new DiffableStringMap(m);

        Map<String, String> m2 = new HashMap<>();
        m2.put("foo", "not-bar");
        m2.put("newkey", "yay");
        m2.put("baz", "eggplant");
        DiffableStringMap dsm2 = new DiffableStringMap(m2);

        Diff<DiffableStringMap> diff = dsm2.diff(dsm);
        assertThat(diff).isInstanceOf((DiffableStringMap.DiffableStringMapDiff.class));
        DiffableStringMap.DiffableStringMapDiff dsmd = (DiffableStringMap.DiffableStringMapDiff) diff;

        assertThat(dsmd.getDeletes()).containsExactly("potato");
        assertThat(dsmd.getDiffs()).isEmpty();
        Map<String, String> upserts = new HashMap<>();
        upserts.put("foo", "not-bar");
        upserts.put("newkey", "yay");
        assertThat(dsmd.getUpserts()).isEqualTo(upserts);

        DiffableStringMap dsm3 = diff.apply(dsm);
        assertThat(dsm3.get("foo")).isEqualTo("not-bar");
        assertThat(dsm3.get("newkey")).isEqualTo("yay");
        assertThat(dsm3.get("baz")).isEqualTo("eggplant");
        assertThat(dsm3.get("potato")).isEqualTo(null);
    }

    public void testRandomDiffing() {
        Map<String, String> m = new HashMap<>();
        m.put("1", "1");
        m.put("2", "2");
        m.put("3", "3");
        DiffableStringMap dsm = new DiffableStringMap(m);
        Map<String, String> expected = new HashMap<>(m);

        for (int i = 0; i < randomIntBetween(5, 50); i++) {
            if (randomBoolean() && expected.size() > 1) {
                expected.remove(randomFrom(expected.keySet()));
            } else if (randomBoolean()) {
                expected.put(randomFrom(expected.keySet()), randomAlphaOfLength(4));
            } else {
                expected.put(randomAlphaOfLength(2), randomAlphaOfLength(4));
            }
            dsm = new DiffableStringMap(expected).diff(dsm).apply(dsm);
        }
        assertThat(expected).isEqualTo(dsm);
    }

    public void testSerialization() throws IOException {
        Map<String, String> m = new HashMap<>();
        // Occasionally have an empty map
        if (frequently()) {
            m.put("foo", "bar");
            m.put("baz", "eggplant");
            m.put("potato", "canon");
        }
        DiffableStringMap dsm = new DiffableStringMap(m);

        BytesStreamOutput bso = new BytesStreamOutput();
        dsm.writeTo(bso);
        DiffableStringMap deserialized = DiffableStringMap.readFrom(bso.bytes().streamInput());
        assertThat(deserialized).isEqualTo(dsm);
    }
}
