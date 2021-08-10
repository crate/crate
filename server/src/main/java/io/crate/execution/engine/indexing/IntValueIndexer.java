/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.crate.execution.engine.indexing;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;

public class IntValueIndexer implements ValueIndexer<Integer> {

    private final String name;

    public IntValueIndexer(String name) {
        this.name = name;
    }

    @Override
    public List<Field> indexValue(Integer value) {
        ArrayList<Field> fields = new ArrayList<>(3);
        int intValue = value.intValue();
        fields.add(new IntPoint(name, intValue));
        fields.add(new StoredField(name, intValue));
        fields.add(new SortedNumericDocValuesField(name, intValue));
        return fields;
    }
}
