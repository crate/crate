/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.lucene;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.Version;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.sql.tree.BitString;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.QueryTester;
import io.crate.types.BitStringType;

public class BitStringQueryTest extends CrateDummyClusterServiceUnitTest {

    private QueryTester.Builder builder(String createTable) throws IOException {
        return new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            createTable
        );
    }

    @Test
    public void test_eq_any_on_bits_uses_termset_query() throws Exception {
        var builder = builder("create table tbl (bits bit(8))");
        BitString bits1 = BitString.ofRawBits("00010110");
        BitString bits2 = BitString.ofRawBits("01110000");
        builder.indexValues("bits", bits1, bits2);

        try (var tester = builder.build()) {
            Query query = tester.toQuery("bits = ANY([B'01001110', B'11111111'])");
            assertThat(query, instanceOf(TermInSetQuery.class));
            assertThat(query.toString(), is("bits:(r [ff])"));

            List<Object> result = tester.runQuery("bits", "bits = ANY([B'01001110', B'11111111'])");
            assertThat(result, Matchers.empty());

            result = tester.runQuery("bits", "bits = ANY([B'00010110', B'11111111'])");
            assertThat(result, Matchers.contains(bits1));

            result = tester.runQuery("bits", "bits = ANY([B'00010110', B'01110000'])");
            assertThat(result, Matchers.contains(bits1, bits2));
        }

    }

    @Test
    public void test_eq_on_bits_uses_term_query() throws Exception {
        var builder = builder("create table tbl (bits bit(8))");
        BitStringType bitStringType = new BitStringType(8);
        Supplier<BitString> dataGenerator = DataTypeTesting.getDataGenerator(bitStringType);
        BitString bits1 = dataGenerator.get();
        BitString bitsNoMatch = dataGenerator.get();
        while (bits1.equals(bitsNoMatch)) {
            bitsNoMatch = dataGenerator.get();
        }
        builder.indexValues("bits", bits1);

        try (var tester = builder.build()) {
            Query query = tester.toQuery("bits = ?", bits1);
            assertThat(query, instanceOf(TermQuery.class));
            assertThat(
                tester.runQuery("bits", "bits = ?", bits1),
                Matchers.contains(bits1)
            );
            assertThat(
                tester.runQuery("bits", "bits = ?", bitsNoMatch),
                Matchers.empty()
            );
        }
    }

    @Test
    public void test_is_not_null_on_bit_column_uses_field_exists_query() throws Exception {
        try (var tester = builder("create table tbl (bits bit(8))").build()) {
            Query query = tester.toQuery("bits is not null");
            assertThat(query.toString(), is("FieldExistsQuery [field=bits]"));
        }
    }

    @Test
    public void test_range_query_on_bit_type_is_not_supported() throws Exception {
        try (var tester = builder("create table tbl (bits bit(8))").build()) {
            assertThrows(
                UnsupportedFunctionException.class,
                () -> tester.toQuery("bits > B'01'"));
        }
    }

}
