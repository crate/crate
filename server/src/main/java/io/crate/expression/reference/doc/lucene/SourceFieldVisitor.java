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

package io.crate.expression.reference.doc.lucene;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.mapper.SourceFieldMapper;

public final class SourceFieldVisitor extends StoredFieldVisitor {

    private boolean done = false;
    private BytesArray source;

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        if (fieldInfo.name.equals(SourceFieldMapper.NAME)) {
            done = true;
            return Status.YES;
        }
        return done ? Status.STOP : Status.NO;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
        assert SourceFieldMapper.NAME.equals(fieldInfo.name) : "Must only receive a source field";
        source = new BytesArray(value);
    }

    public void reset() {
        done = false;
        source = null;
    }

    public BytesArray source() {
        return source;
    }
}
