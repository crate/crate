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

package io.crate.expression.reference.doc.lucene;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.BytesRef;

import io.crate.metadata.doc.SysColumns;

// TODO turn this into something more useful for per-column stored source tables
public class RawFieldVisitor extends StoredFieldVisitor {

    private final Map<String, Object> storedValues = new HashMap<>();

    public Map<String, Object> getStoredValues() {
        return storedValues;
    }

    private static final Set<String> IGNORED_FIELDS = Set.of(SysColumns.Source.RECOVERY_NAME, SysColumns.Names.ID);

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        // ignore id and recovery source
        if (IGNORED_FIELDS.contains(fieldInfo.name)) {
            return Status.NO;
        }
        return Status.YES;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        storedValues.put(fieldInfo.name, new BytesRef(value).toString());
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        storedValues.put(fieldInfo.name, value);
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
        storedValues.put(fieldInfo.name, value);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
        storedValues.put(fieldInfo.name, value);
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
        storedValues.put(fieldInfo.name, value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
        storedValues.put(fieldInfo.name, value);
    }
}
