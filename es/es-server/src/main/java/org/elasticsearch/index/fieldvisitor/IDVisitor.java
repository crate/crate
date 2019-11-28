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

package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.index.mapper.Uid;

import java.nio.charset.StandardCharsets;

public class IDVisitor extends StoredFieldVisitor {

    private boolean canStop = false;
    private String id;
    private final String columnName;

    public IDVisitor(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setCanStop(boolean canStop) {
        this.canStop = canStop;
    }


    public boolean canStop() {
        return canStop;
    }

    public String getId() {
        return id;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        if (canStop) {
            return Status.STOP;
        }
        if (columnName.equals(fieldInfo.name)) {
            canStop = true;
            return Status.YES;
        }
        return Status.NO;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
        assert columnName.equals(fieldInfo.name) : "binaryField must only be called for id";
        id = Uid.decodeId(value);
    }

    @Override
    public void stringField(FieldInfo fieldInfo, byte[] value) {
        assert columnName.equals(fieldInfo.name) : "stringField must only be called for id";
        // Indices prior to CrateDB 3.0 have id stored as string
        id = new String(value, StandardCharsets.UTF_8);
    }

    public void reset() {
        id = null;
        canStop = false;
    }
}
