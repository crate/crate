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

package io.crate.analyze;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.operation.Input;
import io.crate.planner.symbol.DynamicReference;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DispatchingReferenceSymbolVisitor {

    public static interface InnerVisitorContext {

    }

    public static abstract class InnerVisitor<Ctx extends InnerVisitorContext> {

        public abstract Ctx createContext();
        public abstract void visit(Reference ref, Symbol symbol, @Nullable Reference parent, Ctx ctx);
    }

    private final List<InnerVisitor> visitors;
    private final AbstractDataAnalysis analysis;

    public DispatchingReferenceSymbolVisitor(List<InnerVisitor> visitors, AbstractDataAnalysis analysis){
        this.visitors = visitors;
        this.analysis = analysis;
    }

    public List<InnerVisitorContext> process(Reference ref, Symbol value) {
        List<InnerVisitorContext> contexts = new ArrayList<>(this.visitors.size());

        for (InnerVisitor f : this.visitors) {
            InnerVisitorContext ctx = f.createContext();
            contexts.add(ctx);
            f.visit(ref, value, null, ctx);
        }
        if (ref.valueType() == DataTypes.OBJECT && value.symbolType().isValueSymbol()) { // value does not evaluate to null
            // TODO: assert instanceof Map and not null
            Object val = ((Input)value).value();
            if (val != null) {
                processMap((Map<String, Object>) val, ref, contexts);
            }
        }
        return contexts;
    }

    public void processMap(Map<String, Object> map, Reference parent, List<InnerVisitorContext> contexts) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {

            ColumnIdent ident = ColumnIdent.getChild(parent.info().ident().columnIdent(), entry.getKey());
            Reference reference = analysis.allocateReference(
                    new ReferenceIdent(parent.info().ident().tableIdent(), ident));
            Symbol symbol = Literal.newLiteral(DataTypes.guessType(entry.getValue()), entry.getValue());

            if(reference instanceof DynamicReference){
                ((DynamicReference) reference).valueType(DataTypes.guessType(entry.getValue()));
            }

            List<InnerVisitor> visitors1 = this.visitors;
            for (int i = 0; i < visitors1.size(); i++) {
                InnerVisitor f = visitors1.get(i);
                f.visit(reference, symbol, parent, contexts.get(i));
            }
            if (entry.getValue() instanceof Map) {
                processMap((Map<String, Object>) entry.getValue(), reference, contexts);
            }
        }
    }
}
