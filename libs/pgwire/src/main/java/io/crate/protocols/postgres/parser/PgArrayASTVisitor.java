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

package io.crate.protocols.postgres.parser;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.function.Function;

import com.carrotsearch.hppc.ByteArrayList;

import io.crate.protocols.postgres.antlr.v4.PgArrayBaseVisitor;
import io.crate.protocols.postgres.antlr.v4.PgArrayParser;
import io.crate.protocols.postgres.antlr.v4.PgArrayParser.NullContext;
import io.crate.protocols.postgres.antlr.v4.PgArrayParser.QuotedStringContext;
import io.crate.protocols.postgres.antlr.v4.PgArrayParser.UnquotedStringContext;

class PgArrayASTVisitor extends PgArrayBaseVisitor<Object> {

    private final Function<byte[], Object> convert;

    PgArrayASTVisitor(Function<byte[], Object> convert) {
        this.convert = convert;
    }

    @Override
    public Object visitArray(PgArrayParser.ArrayContext ctx) {
        ArrayList<Object> array = new ArrayList<>();
        for (var value : ctx.item()) {
            array.add(value.accept(this));
        }
        return array;
    }

    @Override
    public Object visitUnquotedString(UnquotedStringContext ctx) {
        String text = ctx.getText();
        return convert.apply(text.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Object visitNull(NullContext ctx) {
        return null;
    }

    @Override
    public Object visitQuotedString(QuotedStringContext ctx) {
        String text = ctx.getText();
        String withoutQuotes = text.substring(1, text.length() - 1);
        return convert.apply(removeEscapes(withoutQuotes.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * @param bytes {@code byte[]} that represent an array's item.
     */
    private static byte[] removeEscapes(byte[] bytes) {
        var itemBytes = new ByteArrayList(bytes.length);
        int end = bytes.length - 1;
        for (int i = 0; i <= end; i++) {
            char c = (char) bytes[i];
            if (i < end) {
                char next = (char) bytes[i + 1];
                if (c == '\\' && (next == '\\' || next == '\"')) {
                    i++;
                }
            }
            itemBytes.add(bytes[i]);
        }
        return itemBytes.toArray();
    }
}
