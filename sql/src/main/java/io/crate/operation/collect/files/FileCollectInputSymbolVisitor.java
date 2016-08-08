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

package io.crate.operation.collect.files;

import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.operation.AbstractImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.reference.file.FileLineReferenceResolver;

import java.util.ArrayList;
import java.util.List;

public class FileCollectInputSymbolVisitor
        extends AbstractImplementationSymbolVisitor<FileCollectInputSymbolVisitor.Context> {

    private final FileLineReferenceResolver referenceResolver;

    public FileCollectInputSymbolVisitor(Functions functions,
                                         FileLineReferenceResolver referenceResolver) {
        super(functions);
        this.referenceResolver = referenceResolver;
    }

    @Override
    protected Context newContext() {
        return new Context();
    }

    public static class Context extends AbstractImplementationSymbolVisitor.Context {

        List<LineCollectorExpression<?>> expressions = new ArrayList<>();

        public List<LineCollectorExpression<?>> expressions() {
            return this.expressions;
        }
    }

    @Override
    public Input<?> visitReference(Reference symbol, Context context) {
        LineCollectorExpression<?> implementation = referenceResolver.getImplementation(symbol);
        if (implementation == null) {
            throw new IllegalArgumentException(
                    SymbolFormatter.format("Can't handle Reference \"%s\"", symbol));
        }

        context.expressions.add(implementation);
        return implementation;
    }
}
