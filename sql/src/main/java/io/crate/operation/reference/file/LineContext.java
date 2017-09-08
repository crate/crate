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

package io.crate.operation.reference.file;

import io.crate.metadata.ColumnIdent;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.Map;

public class LineContext {

    private byte[] rawSource;
    private Map<String, Object> parsedSource;

    @Nullable
    public BytesRef sourceAsBytesRef() {
        if (rawSource != null) {
            return new BytesRef(rawSource);
        }
        return null;
    }

    public Map<String, Object> sourceAsMap() {
        if (parsedSource == null) {
            try {
                parsedSource = XContentHelper.convertToMap(new BytesArray(rawSource), false, XContentType.JSON).v2();
            } catch (NullPointerException e) {
                return null;
            }
        }
        return parsedSource;
    }

    public Object get(ColumnIdent columnIdent) {
        // TODO: change interface in order to not compute the path for every row
        if (parsedSource == null) {
            // TODO: optimize if collectorContext has prefetchColumns

            try {
                parsedSource = XContentHelper.convertToMap(new BytesArray(rawSource), false, XContentType.JSON).v2();
            } catch (NullPointerException e) {
                return null;
            }
        }

        LinkedList<String> path = new LinkedList<>(columnIdent.path());
        path.add(0, columnIdent.name());
        Map parentMap = parsedSource;

        while (path.size() > 1) {
            Object o = parentMap.get(path.pollFirst());
            if (o == null) {
                return null;
            }
            assert o instanceof Map : "o must be instance of Map";
            parentMap = (Map) o;
        }

        Object o = parentMap.get(path.peekFirst());
        if (o instanceof String) {
            return new BytesRef((String) o);
        }
        return o;
    }

    public void rawSource(byte[] bytes) {
        this.rawSource = bytes;
        this.parsedSource = null;
    }
}
