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

package io.crate.execution.engine.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.junit.Test;

import io.crate.data.CollectionBucket;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.expression.symbol.Literal;

public class TableFunctionApplierTest {

    @Test
    public void testFunctionsAreApplied() {
        Input<Iterable<Row>> fstFunc = () -> new CollectionBucket(Arrays.asList(
            new Object[]{1},
            new Object[]{2},
            new Object[]{3}
        ));
        Input<Iterable<Row>> sndFunc = () -> new CollectionBucket(Arrays.asList(
            new Object[]{4},
            new Object[]{5}
        ));
        TableFunctionApplier tableFunctionApplier = new TableFunctionApplier(
            Arrays.asList(fstFunc, sndFunc),
            Collections.singletonList(Literal.of(10)),
            Collections.emptyList()
        );
        Iterator<Row> iterator = tableFunctionApplier.apply(new RowN(0));
        assertThat(iterator.next().materialize()).isEqualTo(new Object[]{1, 4, 10});
        assertThat(iterator.next().materialize()).isEqualTo(new Object[]{2, 5, 10});
        assertThat(iterator.next().materialize()).isEqualTo(new Object[]{3, null, 10});
        assertThat(iterator.hasNext()).isFalse();
    }
}
