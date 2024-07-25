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

package io.crate.expression.predicate;

import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.DataTypeTesting.getDataGenerator;
import static io.crate.testing.DataTypeTesting.randomType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.Set;
import java.util.function.Supplier;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.format.Style;
import io.crate.lucene.GenericFunctionQuery;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.QueryTester;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

/*
    A <distinct predicate> tests two values to see whether they are distinct and
    returns either ``TRUE`` or ``FALSE``.
    The two expressions must be comparable. If the attributes are rows, they
    must be of the same degree and each corresponding pair of Fields must have
    comparable <data type>s.
 */
public class DistinctFromTest extends ScalarTestCase {
    @Test
    public void testEvaluateIncomparableDatatypes() {
        assertThatThrownBy(() -> assertEvaluate("3 is distinct from true", isLiteral(false)))
            .isExactlyInstanceOf(ClassCastException.class);
    }

    @Test
    public void testNormalizeSymbolNullNull() {
        // two ``NULL`` values are not distinct from one other
        assertNormalize("null is distinct from null", isLiteral(Boolean.FALSE));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void test_random_data_type() {
        DataType<?> type = DataTypeTesting.randomTypeExcluding(Set.of(DataTypes.GEO_SHAPE));
        Supplier<?> dataGenerator = getDataGenerator(type);

        Object value1 = dataGenerator.get();
        Object value2 = dataGenerator.get();
        // Make sure the two values are distinct. Important for boolean or BitStringType.
        while (((DataType) type).compare(value1, value2) == 0) {
            value2 = dataGenerator.get();
        }

        Literal<?> literal1 = Literal.ofUnchecked(type, value1);
        assertEvaluate("? IS NOT DISTINCT FROM ?", Boolean.TRUE, literal1, literal1);

        Literal<?> literal2 = Literal.ofUnchecked(type, value2);
        assertEvaluate("? IS DISTINCT FROM ?", Boolean.TRUE, literal1, literal2);
    }

    @Test
    public void test_random_data_type_against_null() {
        DataType<?> randomType = randomType();
        var val = Literal.ofUnchecked(randomType, getDataGenerator(randomType).get()).toString(Style.QUALIFIED);

        // Random value IS DISTINCT from another random value.
        assertEvaluate("? IS DISTINCT FROM null", Boolean.TRUE, Literal.of(val));
        assertEvaluate("null IS DISTINCT FROM ?", Boolean.TRUE, Literal.of(val));
    }

    @Test
    public void test_terms_query_on_empty_object() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (str string)");
        builder.indexValue("str", "hello");
        builder.indexValue("str", "Duke");
        builder.indexValue("str", "rules");
        builder.indexValue("str", null);
        try (QueryTester tester = builder.build()) {
            Query query = tester.toQuery("str IS DISTINCT FROM 'hello'");
            assertThat(query)
                .isExactlyInstanceOf(GenericFunctionQuery.class)
                .hasToString("(str IS DISTINCT FROM 'hello')");

            assertThat(tester.runQuery("str", "str IS DISTINCT FROM 'hello'"))
                .containsExactly("Duke", "rules", null);
            assertThat(tester.runQuery("str", "str IS DISTINCT FROM null"))
                .containsExactly("hello", "Duke", "rules");
            assertThat(tester.runQuery("str", "str IS NOT DISTINCT FROM 'hello'"))
                .containsExactly("hello");
        }
    }
}
