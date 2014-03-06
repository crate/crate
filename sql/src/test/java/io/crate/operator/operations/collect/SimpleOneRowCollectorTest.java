/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operator.operations.collect;

import com.google.common.collect.ImmutableList;
import io.crate.operator.Input;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.operator.projectors.Projector;
import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.StringLiteral;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

public class SimpleOneRowCollectorTest {

    private final List<Input<?>> inputs = ImmutableList.<Input<?>>of(
            new BooleanLiteral(true), new StringLiteral("foo"));

    @Test
    public void testCollectOneRow() throws Exception {
        Projector downStream = mock(Projector.class);
        SimpleOneRowCollector collector = new SimpleOneRowCollector(
                inputs,
                Collections.<CollectExpression<?>>emptySet(),
                downStream
        );
        collector.doCollect();
        verify(downStream, times(1)).startProjection();
        verify(downStream, times(1)).setNextRow(true, new BytesRef("foo"));
        verify(downStream, never()).finishProjection();

    }
}
