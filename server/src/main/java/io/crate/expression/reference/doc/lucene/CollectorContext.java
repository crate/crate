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

import java.util.Set;
import java.util.function.UnaryOperator;

import io.crate.metadata.Reference;

public class CollectorContext {

    private final int readerId;
    private final Set<Reference> droppedColumns;
    private final UnaryOperator<String> lookupNameBySourceKey;

    private StoredRowLookup storedRowLookup;

    public CollectorContext(Set<Reference> droppedColumns, UnaryOperator<String> lookupNameBySourceKey) {
        this(-1, droppedColumns, lookupNameBySourceKey);
    }

    public CollectorContext(int readerId, Set<Reference> droppedColumns, UnaryOperator<String> lookupNameBySourceKey) {
        this.readerId = readerId;
        this.droppedColumns = droppedColumns;
        this.lookupNameBySourceKey = lookupNameBySourceKey;
    }

    public int readerId() {
        return readerId;
    }

    public StoredRowLookup storedRowLookup() {
        if (storedRowLookup == null) {
            storedRowLookup = new StoredRowLookup(droppedColumns, lookupNameBySourceKey);
        }
        return storedRowLookup;
    }

    public StoredRowLookup storedRowLookup(Reference ref) {
        if (storedRowLookup == null) {
            storedRowLookup = new StoredRowLookup(droppedColumns, lookupNameBySourceKey);
        }
        return storedRowLookup.registerRef(ref);
    }
}
