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

package io.crate.protocols.postgres.parser;

import com.carrotsearch.hppc.ByteArrayList;
import io.crate.protocols.postgres.antlr.v4.PgArrayBaseVisitor;
import io.crate.protocols.postgres.antlr.v4.PgArrayParser;

import java.util.ArrayList;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

class PgArrayASTVisitor extends PgArrayBaseVisitor<Object> {

    private final Function<byte[], Object> convert;

    PgArrayASTVisitor(Function<byte[], Object> convert) {
        this.convert = convert;
    }

    @Override
    public Object visitArray(PgArrayParser.ArrayContext ctx) {
        ArrayList<Object> array = new ArrayList<>();
        for (var value : ctx.item()) {
            array.add(visit(value));
        }
        return array;
    }

    @Override
    public Object visitValue(PgArrayParser.ValueContext ctx) {
        PgArrayParser.StringContext stringContext = ctx.string();
        if (stringContext == null) {
            return convert.apply(processArrayItem(ctx.getText().getBytes(UTF_8)));
        }
        Object value = visit(stringContext);
        if (value == null) {
            return null;
        } else {
            return convert.apply(processArrayItem(((String) value).getBytes(UTF_8)));
        }
    }

    @Override
    public Object visitQuotedString(PgArrayParser.QuotedStringContext ctx) {
        String text = ctx.getText();
        // Drop the quotes
        return text.substring(1, text.length() - 1);
    }

    @Override
    public Object visitUnquotedString(PgArrayParser.UnquotedStringContext ctx) {
        String text = ctx.getText();
        return "null".equalsIgnoreCase(text) ? null : text;
    }

    @Override
    public Object visitNull(PgArrayParser.NullContext ctx) {
        return null;
    }

    /**
     * Processes an array's item by skipping escape characters
     * and unquoting the item if it is needed.
     *
     * @param bytes {@code byte[]} that represent an array's item.
     */
    private static byte[] processArrayItem(byte[] bytes) {
        var itemBytes = new ByteArrayList();
        int start = 0, end = bytes.length - 1;
        for (int i = start; i <= end; i++) {
            if (i < end && (char) bytes[i] == '\\'
                && ((char) bytes[i + 1] == '\\' || (char) bytes[i + 1] == '\"')) {
                i++;
            }
            itemBytes.add(bytes[i]);
        }
        return itemBytes.toArray();
    }
}
