/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class IndexWriterProjectorUnitTest {

    @Test(expected = IllegalStateException.class)
    public void testExceptionBubbling() throws Throwable {
        CollectingProjector collectingProjector = new CollectingProjector();
        InputCollectExpression<Object> idInput = new InputCollectExpression<>(0);
        InputCollectExpression<Object> sourceInput = new InputCollectExpression<>(1);
        CollectExpression[] collectExpressions = new CollectExpression[]{ idInput, sourceInput };

        final IndexWriterProjector indexWriter = new IndexWriterProjector(
                null,
                "bulk_import",
                idInput,
                null,
                sourceInput,
                collectExpressions,
                20,
                2
        );
        indexWriter.downstream(collectingProjector);
        indexWriter.registerUpstream(null);
        indexWriter.upstreamFailed(new IllegalStateException("my dummy exception"));

        try {
            collectingProjector.result().get();
        } catch (InterruptedException | ExecutionException e) {
            throw e.getCause();
        }
    }
}
