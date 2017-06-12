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

package io.crate.analyze;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.crate.analyze.symbol.Field;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.metadata.Path;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Fields {

    private final Multimap<String, Field> fieldsMap = HashMultimap.create();
    private final List<Field> fieldsList;

    public Fields(int expectedSize) {
        fieldsList = new ArrayList<>(expectedSize);
    }

    public void add(Path key, Field value) {
        fieldsMap.put(key.outputName(), value);
        fieldsList.add(value);
    }

    public Field get(Path key) {
        Collection<Field> fieldList = fieldsMap.get(key.outputName());
        if (fieldList.size() > 1) {
            throw new AmbiguousColumnAliasException(key.outputName(), fieldList);
        }
        if (fieldList.isEmpty()) {
            return null;
        }
        return fieldList.iterator().next();
    }

    public List<Field> asList() {
        return fieldsList;
    }

    @Override
    public String toString() {
        return "Fields{" + fieldsList + '}';
    }
}
