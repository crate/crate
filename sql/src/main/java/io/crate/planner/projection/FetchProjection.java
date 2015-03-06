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

package io.crate.planner.projection;

import io.crate.operation.projectors.FetchProjector;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FetchProjection extends Projection {

    public static final ProjectionFactory<FetchProjection> FACTORY = new ProjectionFactory<FetchProjection>() {
        @Override
        public FetchProjection newInstance() {
            return new FetchProjection();
        }
    };

    private Symbol docIdSymbol;
    private List<Symbol> inputSymbols;
    private List<Symbol> outputSymbols;
    private int numUpstreams;
    private int bulkSize;

    private FetchProjection() {
    }

    public FetchProjection(Symbol docIdSymbol,
                           List<Symbol> inputSymbols,
                           List<Symbol> outputSymbols,
                           int numUpstreams) {
        this(docIdSymbol, inputSymbols, outputSymbols, numUpstreams, FetchProjector.NO_BULK_REQUESTS);
    }

    public FetchProjection(Symbol docIdSymbol,
                           List<Symbol> inputSymbols,
                           List<Symbol> outputSymbols,
                           int numUpstreams,
                           int bulkSize) {
        this.docIdSymbol = docIdSymbol;
        this.inputSymbols = inputSymbols;
        this.outputSymbols = outputSymbols;
        this.numUpstreams = numUpstreams;
        this.bulkSize = bulkSize;
    }

    public Symbol docIdSymbol() {
        return docIdSymbol;
    }

    public List<Symbol> inputSymbols() {
        return inputSymbols;
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    public int numUpstreams() {
        return numUpstreams;
    }

    public int bulkSize() {
        return bulkSize;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.FETCH;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitFetchProjection(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputSymbols;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FetchProjection that = (FetchProjection) o;

        if (bulkSize != that.bulkSize) return false;
        if (numUpstreams != that.numUpstreams) return false;
        if (!docIdSymbol.equals(that.docIdSymbol)) return false;
        if (!inputSymbols.equals(that.inputSymbols)) return false;
        if (!outputSymbols.equals(that.outputSymbols)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + docIdSymbol.hashCode();
        result = 31 * result + inputSymbols.hashCode();
        result = 31 * result + outputSymbols.hashCode();
        result = 31 * result + numUpstreams;
        result = 31 * result + bulkSize;
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        docIdSymbol = Symbol.fromStream(in);
        int inputSymbolsSize = in.readVInt();
        inputSymbols = new ArrayList<>(inputSymbolsSize);
        for (int i = 0; i < inputSymbolsSize; i++) {
            inputSymbols.add(Symbol.fromStream(in));
        }
        int outputSymbolsSize = in.readVInt();
        outputSymbols = new ArrayList<>(outputSymbolsSize);
        for (int i = 0; i < outputSymbolsSize; i++) {
            outputSymbols.add(Symbol.fromStream(in));
        }
        numUpstreams = in.readVInt();
        bulkSize = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbol.toStream(docIdSymbol, out);
        out.writeVInt(inputSymbols.size());
        for (Symbol symbol : inputSymbols) {
            Symbol.toStream(symbol, out);
        }
        out.writeVInt(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            Symbol.toStream(symbol, out);
        }
        out.writeVInt(numUpstreams);
        out.writeVInt(bulkSize);
    }
}
