/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.projectors.fetch;

import io.crate.analyze.symbol.Symbol;
import io.crate.data.AsyncOperationBatchIterator;
import io.crate.data.BatchIterator;
import io.crate.data.Projector;
import io.crate.metadata.Functions;

import java.util.List;

public class FetchProjector implements Projector {

    private final FetchOperation fetchOperation;
    private final Functions functions;
    private final List<Symbol> outputSymbols;
    private final FetchProjectorContext fetchProjectorContext;
    private final int fetchSize;

    public FetchProjector(FetchOperation fetchOperation,
                          Functions functions,
                          List<Symbol> outputSymbols,
                          FetchProjectorContext fetchProjectorContext,
                          int fetchSize) {
        this.fetchOperation = fetchOperation;
        this.functions = functions;
        this.outputSymbols = outputSymbols;
        this.fetchProjectorContext = fetchProjectorContext;
        this.fetchSize = fetchSize;
    }

    @Override
    public BatchIterator apply(BatchIterator batchIterator) {
        return new AsyncOperationBatchIterator(
            batchIterator,
            outputSymbols.size(),
            new FetchBatchAccumulator(
                fetchOperation,
                functions,
                outputSymbols,
                fetchProjectorContext,
                fetchSize
            )
        );
    }
}
