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

import org.apache.lucene.search.Query;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class ThreeValuedLogicQueryBuilderTest extends LuceneQueryBuilderTest {

    @Test
    public void testNotAnyEqWith3vl() {
        assertThat(
            convert("NOT 10 = ANY(y_array)").toString(),
            is("+(+*:* -y_array:[10 TO 10]) +(+*:* -((10::bigint = ANY(y_array)) IS NULL))")
        );
        assertThat
            (
            convert("NOT d = ANY([1,2,3])").toString(),
            is("+(+*:* -d:{1.0 2.0 3.0}) +(+*:* -((d = ANY([1.0, 2.0, 3.0])) IS NULL))")
        );
    }

    @Test
    public void testNotAnyEqWithout3vl() {
        assertThat(
            convert("NOT ignore3vl(20 = ANY(y_array))").toString(),
            is("+(+*:* -y_array:[20 TO 20])")
        );
        assertThat(
            convert("NOT ignore3vl(d = ANY([1,2,3]))").toString(),
            is("+(+*:* -d:{1.0 2.0 3.0})")
        );
    }

    @Test
    public void testComplexOperatorTreeWith3vlAndIgnore3vl() {
        assertThat(
            convert("NOT name = 'foo' AND NOT ignore3vl(name = 'bar')").toString(),
            is("+(+(+*:* -name:foo) +ConstantScore(DocValuesFieldExistsQuery [field=name])) +(+(+*:* -name:bar))")
        );
    }

    @Test
    public void testNullIsReplacedWithFalseToCreateOptimizedQuery() {
        Query q1 = convert("null or name = 'foo'");
        Query q2 = convert("name = 'foo'");

        assertThat(q1, is(q2));
        assertThat(q1.toString(), is("name:foo"));
    }
}
