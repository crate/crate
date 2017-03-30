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

package io.crate.operation.collect.collectors;

import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CollectorFieldsVisitor extends FieldsVisitor {

    private final Set<String> requiredFields;
    private boolean required = false;

    public CollectorFieldsVisitor(int size) {
        super(false);
        requiredFields = new HashSet<>(size);
    }

    public boolean addField(String name) {
        required = true;
        return requiredFields.add(name);
    }

    public boolean required() {
        return required;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (SourceFieldMapper.NAME.equals(fieldInfo.name)) {
            return Status.YES;
        }
        return requiredFields.contains(fieldInfo.name) ? Status.YES : Status.NO;
    }

    public void required(boolean required) {
        this.required = required;
    }
}
