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

package io.crate.protocols.postgres.types;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.common.collections.Lists;
import io.netty.buffer.ByteBuf;

public class PgOidVectorType extends PGType<List<Integer>> {

    public static final PgOidVectorType INSTANCE = new PgOidVectorType();
    private static final int OID = 30;

    PgOidVectorType() {
        super(OID, -1, -1, "oidvector");
    }

    @Override
    public int typArray() {
        return 1013;
    }

    @Override
    public int typElem() {
        return OidType.OID;
    }

    @Override
    public String typeCategory() {
        return TypeCategory.ARRAY.code();
    }

    @Override
    public String type() {
        return Type.BASE.code();
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public int writeAsBinary(ByteBuf buffer, List<Integer> value) {
        return PGArray.INT4_ARRAY.writeAsBinary(buffer, (List) value);
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public List<Integer> readBinaryValue(ByteBuf buffer, int valueLength) {
        return (List<Integer>) (List) PGArray.INT4_ARRAY.readBinaryValue(buffer, valueLength);
    }

    @Override
    byte[] encodeAsUTF8Text(List<Integer> value) {
        return Lists.joinOn(" ", value, x -> Integer.toString(x)).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    List<Integer> decodeUTF8Text(byte[] bytes) {
        String string = new String(bytes, StandardCharsets.UTF_8);
        return listFromOidVectorString(string);
    }

    @VisibleForTesting
    public static List<Integer> listFromOidVectorString(String value) {
        var tokenizer = new StringTokenizer(value, " ");
        ArrayList<Integer> oids = new ArrayList<>();
        while (tokenizer.hasMoreTokens()) {
            oids.add(Integer.parseInt(tokenizer.nextToken()));
        }
        return oids;
    }
}
