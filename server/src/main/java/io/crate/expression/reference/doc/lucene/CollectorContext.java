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

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;

public class CollectorContext {

    private final int readerId;
    private final Set<ColumnIdent> droppedColumns;

    private SourceLookup sourceLookup;

    public CollectorContext(Set<ColumnIdent> droppedColumns) {
        this(-1, droppedColumns);
    }

    public CollectorContext(int readerId, Set<ColumnIdent> droppedColumns) {
        this.readerId = readerId;
        this.droppedColumns = droppedColumns;
    }

    public int readerId() {
        return readerId;
    }

    public Set<ColumnIdent> droppedColumns() {
        return droppedColumns;
    }

    public SourceLookup sourceLookup() {
        if (sourceLookup == null) {
            sourceLookup = new SourceLookup(droppedColumns);
        }
        return sourceLookup;
    }

    public SourceLookup sourceLookup(Reference ref) {
        if (sourceLookup == null) {
            sourceLookup = new SourceLookup(droppedColumns);
        }
        return sourceLookup.registerRef(ref);
    }
}
