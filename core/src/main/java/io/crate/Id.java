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

package io.crate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Id implements Streamable {

    private final List<String> values = new ArrayList<>();
    private final boolean create;

    public Id() {
        create = true;
    }

    public Id(boolean create) {
        this.create = create;
    }

    public boolean applyValues(List<String> primaryKeys, List<String> primaryKeyValues,
                            String clusteredBy) {
        if (primaryKeys.size() == 1 && primaryKeys.get(0).equals("_id") && create) {
            values.add(Strings.randomBase64UUID());
        } else {
            if (primaryKeys.size() != primaryKeyValues.size()) {
                // Primary key count does not match, cannot compute id
                if (create) {
                    throw new UnsupportedOperationException("Missing required primary key values");
                }
                return false;
            }
            for (int i=0; i<primaryKeys.size(); i++)  {
                String primaryKeyValue = primaryKeyValues.get(i);
                if (primaryKeyValue == null) {
                    // Missing primary key value, cannot compute id
                    return false;
                }
                if (primaryKeys.get(i).equals(clusteredBy)) {
                    // clusteredBy value must always be first
                    values.add(0, primaryKeyValue);
                } else {
                    values.add(primaryKeyValue);
                }
            }
        }

        return true;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        for (int i=0; i < size; i++) {
            values.add(in.readBytesRef().utf8ToString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(values.size());
        for (String value : values) {
            out.writeBytesRef(new BytesRef(value));
        }
    }

    public BytesReference bytes() {
        if (values.size() == 0) {
            return null;
        }
        BytesStreamOutput out = new BytesStreamOutput();
        try {
            writeTo(out);
            out.close();
        } catch (IOException e) {
            //
        }
        return out.bytes();
    }

    public String toString() {
        if (values.size() == 0) {
            return null;
        } else if (values.size() == 1) {
            return values.get(0);
        }
        BytesReference bytesReference = bytes();
        if (bytes() == null) {
            return null;
        }
        return Base64.encodeBytes(bytesReference.toBytes());
    }

    public List<String> values() {
        return values;
    }

    /**
     * Creates an Id object from a Base64 encoded string input.
     * WARNING: Using this method with non-base64 encoded string input results in unpredictable
     * id values!
     *
     * @param base64encodedString
     * @return
     * @throws IOException
     */
    public static Id fromString(String base64encodedString) throws IOException {
        assert base64encodedString != null;
        byte[] inputBytes = Base64.decode(base64encodedString);
        BytesStreamInput in = new BytesStreamInput(inputBytes, true);
        Id id = new Id(false);
        id.readFrom(in);
        return id;
    }

}
