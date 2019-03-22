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

package io.crate.expression.reference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

public final class Doc {

    private final Map<String, Object> source;
    private final Supplier<String> raw;
    private final int docId;
    private final String index;
    private final String id;
    private final long version;

    public Doc(int docId,
               String index,
               String id,
               long version,
               Map<String, Object> source,
               Supplier<String> raw) {
        this.docId = docId;
        this.index = index;
        this.id = id;
        this.version = version;
        this.source = source;
        this.raw = raw;
    }

    public int docId() {
        return docId;
    }

    public long getVersion() {
        return version;
    }

    public String getId() {
        return id;
    }

    public String getRaw() {
        return raw.get();
    }

    public Map<String, Object> getSource() {
        return source;
    }

    public String getIndex() {
        return index;
    }

    public Doc withUpdatedSource(Map<String, Object> updatedSource) {
        return new Doc(
            docId,
            index,
            id,
            version,
            updatedSource,
            () -> {
                try {
                    return Strings.toString(XContentFactory.jsonBuilder().map(updatedSource));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        );
    }
}
