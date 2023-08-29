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

package io.crate.lucene;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.sql.tree.BitString;

public class BitStringEqQueryTest extends LuceneQueryBuilderTest {
    private static final BitString BIT_STRING = BitString.ofBitString("B'001'");
    private static final BytesRef BIT_STRING_IN_BYTES_REF = new BytesRef(BIT_STRING.bitSet().toByteArray());
    
    @Override
    protected String createStmt() {
        // `columnstore = false` is not supported
        return """
                create table m (
                a1 bit(3),
                a2 bit(3) index off
            )
            """;
    }

    @Test
    public void test_BitStringEqQuery_termQuery() {
        Query query = convert("a1 = " + BIT_STRING);
        assertThat(query).isExactlyInstanceOf(TermQuery.class);
        Term term = ((TermQuery) query).getTerm();
        assertThat(term.field()).endsWith("a1");
        assertThat(term.bytes()).isEqualTo(BIT_STRING_IN_BYTES_REF);

        query = convert("a2 = " + BIT_STRING);
        // SortedSetDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedSetDocValuesRangeQuery");
        assertThat(query).hasToString("a2:[" + BIT_STRING_IN_BYTES_REF + " TO " + BIT_STRING_IN_BYTES_REF + "]");
    }

    @Test
    public void test_BitStringEqQuery_rangeQuery() {
        assertThatThrownBy(
            () -> convert("a1 < " + BIT_STRING))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: (doc.m.a1 < B'001'), no overload found for matching argument types: (bit, bit).");

        assertThatThrownBy(
            () -> convert("a2 >= " + BIT_STRING))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: (doc.m.a2 >= B'001'), no overload found for matching argument types: (bit, bit).");
    }
}
