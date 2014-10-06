/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import io.crate.metadata.TableIdent;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class ReferencedTables {

    public static final ReferencedTables EMPTY = new ReferencedTables();

    private final Set<TableIdent> idents;

    public ReferencedTables(TableIdent... idents) {
        this.idents = new HashSet<>();
        Collections.addAll(this.idents, idents);
    }

    public void merge(ReferencedTables tables) {
        idents.addAll(tables.idents);
    }

    public boolean referencesMany() {
        return idents.size() > 1;
    }

    public boolean referencesOnly(TableIdent ident) {
        return idents.size() == 1 && references(ident);
    }

    public boolean references(TableIdent ident) {
        return idents.contains(ident);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReferencedTables that = (ReferencedTables) o;

        if (!idents.equals(that.idents)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return idents.hashCode();
    }
}
