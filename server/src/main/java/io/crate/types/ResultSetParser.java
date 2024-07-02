/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.types;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.impl.PointImpl;
import org.postgresql.geometric.PGpoint;
import org.postgresql.util.PGobject;

import io.crate.protocols.postgres.types.PGArray;
import io.crate.protocols.postgres.types.PgOidVectorType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ResultSetParser {
    /**
     * retrieve the same type of object from the resultSet as the CrateClient would return
     */
    public static Object getObject(ResultSet resultSet, int i, String typeName) throws SQLException {
        Object value;
        int columnIndex = i + 1;
        switch (typeName) {
            // need to use explicit `get<Type>` for some because getObject would return a wrong type.
            // E.g. int2 would return Integer instead of short.
            case "int2":
                Integer intValue = (Integer) resultSet.getObject(columnIndex);
                if (intValue == null) {
                    return null;
                }
                value = intValue.shortValue();
                break;
            case "_char": {
                Array array = resultSet.getArray(columnIndex);
                if (array == null) {
                    return null;
                }
                ArrayList<Byte> elements = new ArrayList<>();
                for (Object o : ((Object[]) array.getArray())) {
                    elements.add(Byte.parseByte((String) o));
                }
                return elements;
            }
            case "oidvector": {
                String textval = resultSet.getString(columnIndex);
                if (textval == null) {
                    return null;
                }
                return PgOidVectorType.listFromOidVectorString(textval);
            }
            case "char":
                String strValue = resultSet.getString(columnIndex);
                if (strValue == null) {
                    return null;
                }
                return Byte.valueOf(strValue);
            case "byte":
                value = resultSet.getByte(columnIndex);
                break;
            case "_json", "_jsonb": {
                Array array = resultSet.getArray(columnIndex);
                if (array == null) {
                    return null;
                }
                ArrayList<Object> jsonObjects = new ArrayList<>();
                for (Object item : (Object[]) array.getArray()) {
                    jsonObjects.add(jsonToObject((String) item));
                }
                value = jsonObjects;
                break;
            }
            case "json", "jsonb":
                String json = resultSet.getString(columnIndex);
                value = jsonToObject(json);
                break;
            case "point":
                PGpoint pGpoint = resultSet.getObject(columnIndex, PGpoint.class);
                value = new PointImpl(pGpoint.x, pGpoint.y, JtsSpatialContext.GEO);
                break;
            case "record":
                value = resultSet.getObject(columnIndex, PGobject.class).getValue();
                break;
            case "_bit":
                String pgBitStringArray = resultSet.getString(columnIndex);
                if (pgBitStringArray == null) {
                    return null;
                }
                byte[] bytes = pgBitStringArray.getBytes(StandardCharsets.UTF_8);
                ByteBuf buf = Unpooled.wrappedBuffer(bytes);
                value = PGArray.BIT_ARRAY.readTextValue(buf, bytes.length);
                buf.release();
                break;

            default:
                value = resultSet.getObject(columnIndex);
                break;
        }
        if (value instanceof Timestamp) {
            value = ((Timestamp) value).getTime();
        } else if (value instanceof Array) {
            value = Arrays.asList(((Object[]) ((Array) value).getArray()));
        }
        return value;
    }

    private static Object jsonToObject(String json) {
        try {
            if (json != null) {
                byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
                XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
                    NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes);
                if (bytes.length >= 1 && bytes[0] == '[') {
                    parser.nextToken();
                    return parser.list();
                } else {
                    return parser.mapOrdered();
                }
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
