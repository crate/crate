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

package io.crate.exceptions;

import io.crate.metadata.TableIdent;

import java.util.Collection;
import java.util.stream.Collectors;

public class RelationsUnknown extends ResourceUnknownException implements TableScopeException {

    private final Iterable<TableIdent> relations;

    public RelationsUnknown(Collection<TableIdent> relations) {
        super(errorMsg(relations));
        this.relations = relations;
    }

    @Override
    public Iterable<TableIdent> getTableIdents() {
        return relations;
    }

    private static String errorMsg(Collection<TableIdent> relations) {
        return "Relations not found: " + relations.stream().map(TableIdent::sqlFqn).collect(Collectors.joining(", "));
    }
}
