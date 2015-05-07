/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.base.MoreObjects;
import io.crate.metadata.ColumnIdent;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Id {

    private final List<BytesRef> values;
    private String routingValue;

    // used to avoid bytesRef/string conversion if there is just one primary key used for IndexRequests
    private String singleStringValue = null;

    public Id(List<ColumnIdent> primaryKeys, List<BytesRef> primaryKeyValues,
              ColumnIdent clusteredBy) {
        this(primaryKeys, primaryKeyValues, clusteredBy, true);
    }

    public Id(List<ColumnIdent> primaryKeys, List<BytesRef> primaryKeyValues,
              ColumnIdent clusteredBy, boolean create) {
        values = new ArrayList<>(primaryKeys.size());
        if (primaryKeys.size() == 1 && primaryKeys.get(0).name().equals("_id") && create) {
            singleStringValue = Strings.base64UUID();
        } else {
            singleStringValue = null;
            if (primaryKeys.size() != primaryKeyValues.size()) {
                // Primary key count does not match, cannot compute id
                if (create) {
                    throw new UnsupportedOperationException("Missing required primary key values");
                }
                return;
            }
            for (int i=0; i<primaryKeys.size(); i++)  {
                BytesRef primaryKeyValue = primaryKeyValues.get(i);
                if (primaryKeyValue == null) {
                    // Missing primary key value, cannot compute id
                    return;
                }
                if (primaryKeys.get(i).equals(clusteredBy)) {
                    // clusteredBy value must always be first
                    values.add(0, primaryKeyValue);
                    routingValue = primaryKeyValue.utf8ToString();
                } else {
                    values.add(primaryKeyValue);
                }
            }
        }

    }

    public String routingValue(){
        return MoreObjects.firstNonNull(routingValue, stringValue());
    }

    public boolean isValid() {
        return values.size() > 0 || singleStringValue != null;
    }

    private void encodeValues(StreamOutput out) throws IOException {
        out.writeVInt(values.size());
        for (BytesRef value : values) {
            out.writeBytesRef(value);
        }
    }

    private BytesReference bytes() {
        assert values.size() > 0;
        BytesStreamOutput out = new BytesStreamOutput(estimateSize(values));
        try {
            encodeValues(out);
            out.close();
        } catch (IOException e) {
            //
        }
        return out.bytes();
    }

    /**
     * estimates the size the bytesRef values will take if written onto a StreamOutput using the String streamer
     */
    private int estimateSize(List<BytesRef> values) {
        int expectedEncodedSize = 0;
        for (BytesRef value : values) {
            // 5 bytes for the value of the length itself using vInt
            expectedEncodedSize += 5 + (value != null ? value.length : 0);
        }
        return expectedEncodedSize;
    }

    @Nullable
    public String stringValue() {
        if (singleStringValue != null) {
            return singleStringValue;
        }
        if (values.size() == 0) {
            return null;
        } else if (values.size() == 1) {
            return BytesRefs.toString(values.get(0));
        }
        return Base64.encodeBytes(bytes().toBytes());
    }

    @Nullable
    public String toString() {
        return stringValue();
    }

    public List<BytesRef> values() {
        if (singleStringValue != null && values.size() == 0) {
            // convert singleStringValue lazy..
            values.add(new BytesRef(singleStringValue));
        }
        return values;
    }
}
