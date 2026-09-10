/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

public class DistinctValueSetTest extends ESTestCase {
    private DistinctValueSet set1;
    private DistinctValueSet set2;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        set1 = new DistinctValueSet();
        set2 = new DistinctValueSet();
    }

    @Test
    public void test_all_unique_values() {
        set1.add("a");
        set1.add("b");
        assertThat(set1.distinctValues()).isEqualTo(2);
        assertThat(set1.singletons()).isEqualTo(2);
    }

    @Test
    public void test_duplicate_values_are_not_singletons() {
        set1.add("a");
        set1.add("a");
        assertThat(set1.distinctValues()).isEqualTo(1);
        assertThat(set1.singletons()).isEqualTo(0);

        set1.add("b");
        assertThat(set1.distinctValues()).isEqualTo(2);
        assertThat(set1.singletons()).isEqualTo(1);

        set1.add("a");
        assertThat(set1.distinctValues()).isEqualTo(2);
        assertThat(set1.singletons()).isEqualTo(1);
    }

    @Test
    public void test_empty_strings_are_counted() {
        set1.add("");
        set1.add("");
        assertThat(set1.distinctValues()).isEqualTo(1);
        assertThat(set1.singletons()).isEqualTo(0);
    }

    @Test
    public void test_merge_non_overlapping() {
        set1.add("a");
        set2.add("b");

        set1.merge(set2);
        assertThat(set1.distinctValues()).isEqualTo(2);
        assertThat(set1.singletons()).isEqualTo(2);
    }

    @Test
    public void test_merge_value_seen_once_in_each_set() {
        set1.add("a");
        set2.add("a");

        set1.merge(set2);
        assertThat(set1.distinctValues()).isEqualTo(1);
        assertThat(set1.singletons()).isEqualTo(0);
    }

    @Test
    public void test_merge_value_seen_multiple_times() {
        set1.add("a");
        set2.add("a");
        set2.add("a");

        set1.merge(set2);
        assertThat(set1.distinctValues()).isEqualTo(1);
        assertThat(set1.singletons()).isEqualTo(0);
    }

    @Test
    public void test_merge_keeps_duplicates() {
        set1.add("a");
        set1.add("a");
        set2.add("b");

        set1.merge(set2);
        assertThat(set1.distinctValues()).isEqualTo(2);
        assertThat(set1.singletons()).isEqualTo(1);
    }

    @Test
    public void test_numeric_values() {
        set1.add(1);
        set1.add(1);
        set1.add(2);
        set1.add(3);

        set1.add(2);
        set1.add(4);
        set1.add(4);

        set1.merge(set2);
        assertThat(set1.distinctValues()).isEqualTo(4);
        assertThat(set1.singletons()).isEqualTo(1);
    }

    @Test
    public void test_double_values() {
        set1.add(Double.NaN);
        set1.add(Double.NaN);

        assertThat(set1.distinctValues()).isEqualTo(1);
        assertThat(set1.singletons()).isEqualTo(0);

        set1.add(Double.POSITIVE_INFINITY);
        set1.add(Double.NEGATIVE_INFINITY);

        assertThat(set1.distinctValues()).isEqualTo(3);
        assertThat(set1.singletons()).isEqualTo(2);

        set1.add(-0.0d);
        set1.add(0.0d);

        assertThat(set1.distinctValues()).isEqualTo(5);
        assertThat(set1.singletons()).isEqualTo(4);
    }

    @Test
    public void test_streaming() throws IOException {
        set1.add("a");
        set1.add("a");
        set1.add("b");

        BytesStreamOutput out = new BytesStreamOutput();
        set1.writeTo(out);
        DistinctValueSet deserialized = DistinctValueSet.fromStream(out.bytes().streamInput());

        assertThat(deserialized.distinctValues()).isEqualTo(2);
        assertThat(deserialized.singletons()).isEqualTo(1);
    }
}
