/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.SchemaInfo;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Update;

import java.util.HashMap;
import java.util.Map;

public class UpdateAnalysis extends Analysis {

    private Update updateStatement;
    private Map<Reference, Symbol> assignments = new HashMap<>();

    public UpdateAnalysis(ReferenceInfos referenceInfos,
                          Functions functions,
                          Object[] parameters,
                          ReferenceResolver referenceResolver) {
        super(referenceInfos, functions, parameters, referenceResolver);
    }

    @Override
    public Type type() {
        return Type.UPDATE;
    }

    public Update updateStatement() {
        return updateStatement;
    }

    public void updateStatement(Update updateStatement) {
        this.updateStatement = updateStatement;
    }

    public Map<Reference, Symbol> assignments() {
        return assignments;
    }

    public void addAssignement(Reference reference, Symbol value) {
        if (assignments.containsKey(reference)) {
            throw new IllegalArgumentException(String.format("reference repeated %s", reference.info().ident().columnIdent().fqn()));
        }
        assignments.put(reference, value);
    }

    @Override
    public void table(TableIdent tableIdent) {
        super.table(tableIdent);
        SchemaInfo schema = referenceInfos.getSchemaInfo(tableIdent.schema());
        // null schema already caught by TableUnknownException
        if (schema != null && schema.systemSchema()) {
            throw new UnsupportedOperationException(
                    String.format("tables of schema '%s' are read only.", tableIdent.schema()));
        }
        if (table().isAlias()) {
            throw new IllegalArgumentException("Table alias not allowed in UPDATE statement.");
        }
    }
}
