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

package io.crate.executor.transport.task.elasticsearch;

import io.crate.action.sql.SQLResponse;
import io.crate.analyze.symbol.*;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class ScalarScriptArgSymbolVisitor extends SymbolVisitor<XContentBuilder, Void> {

    public Void process(Symbol symbol, XContentBuilder builder) {
        symbol.accept(this, builder);
        return null;
    }

    @Override
    public Void visitReference(Reference symbol, XContentBuilder builder) {
        try {
            builder.startObject()
                    .field("field_name", symbol.info().ident().columnIdent().fqn())
                    .field("type");
            SQLResponse.toXContentNestedDataType(builder, symbol.valueType());
            builder.endObject();
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
        return null;
    }

    @Override
    public Void visitFunction(Function symbol, XContentBuilder builder) {

        try {
            builder.startObject()
                    .field("scalar_name", symbol.info().ident().name())
                    .field("type");
            SQLResponse.toXContentNestedDataType(builder, symbol.valueType());
            if (!symbol.arguments().isEmpty()) {
                builder.startArray("args");
                for (Symbol argument : symbol.arguments()) {
                    process(argument, builder);
                }
                builder.endArray();
            }
            builder.endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public Void visitLiteral(Literal symbol, XContentBuilder builder) {

        try {
            builder.startObject()
                    .field("value", symbol.value())
                    .field("type");
            SQLResponse.toXContentNestedDataType(builder, symbol.valueType());
            builder.endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
