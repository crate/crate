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

package io.crate.planner.fetch;

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.RelationSource;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.*;
import io.crate.metadata.DocReferences;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.sql.tree.QualifiedName;

import java.util.*;

class MultiSourceFetchPushDown {

    private final MultiSourceSelect statement;

    private List<Symbol> remainingOutputs;
    private Map<TableIdent, FetchSource> fetchSources;

    MultiSourceFetchPushDown(MultiSourceSelect statement) {
        this.statement = statement;
        this.fetchSources = new HashMap<>(statement.sources().size());
    }

    Map<TableIdent, FetchSource> fetchSources() {
        return fetchSources;
    }

    List<Symbol> remainingOutputs() {
        return remainingOutputs;
    }

    void process() {
        remainingOutputs = statement.querySpec().outputs();
        statement.querySpec().outputs(new ArrayList<>());

        HashMap<Symbol, FetchReference> fetchRefByOriginalSymbol = new HashMap<>();
        ArrayList<Symbol> mssOutputs = new ArrayList<>(
            statement.sources().size() + statement.requiredForQuery().size());

        for (Map.Entry<QualifiedName, RelationSource> entry : statement.sources().entrySet()) {
            RelationSource source = entry.getValue();
            if (!(source.relation() instanceof DocTableRelation)) {
                mssOutputs.addAll(source.querySpec().outputs());
                continue;
            }

            DocTableRelation rel = (DocTableRelation) source.relation();
            HashSet<Field> canBeFetched = filterByRelation(statement.canBeFetched(), rel);
            if (!canBeFetched.isEmpty()) {

                Field fetchIdColumn = rel.getField(DocSysColumns.FETCHID);
                mssOutputs.add(fetchIdColumn);
                InputColumn fetchIdInput = new InputColumn(mssOutputs.size() - 1);

                ArrayList<Symbol> qtOutputs = new ArrayList<>(
                    source.querySpec().outputs().size() - canBeFetched.size() + 1);
                qtOutputs.add(fetchIdColumn);

                for (Symbol output : source.querySpec().outputs()) {
                    if (!canBeFetched.contains(output)) {
                        qtOutputs.add(output);
                        mssOutputs.add(output);
                    }
                }
                for (Field field : canBeFetched) {
                    FetchReference fr = new FetchReference(
                        fetchIdInput, DocReferences.toSourceLookup(rel.resolveField(field)));
                    allocateFetchedReference(fr, rel);
                    fetchRefByOriginalSymbol.put(field, fr);
                }
                source.querySpec().outputs(qtOutputs);
            } else {
                mssOutputs.addAll(source.querySpec().outputs());
            }
        }

        statement.querySpec().outputs(mssOutputs);
        MappingSymbolVisitor.inPlace().processInplace(remainingOutputs, fetchRefByOriginalSymbol);
        if (statement.querySpec().orderBy().isPresent()) {
            MappingSymbolVisitor.inPlace().processInplace(statement.querySpec().orderBy().get().orderBySymbols(), fetchRefByOriginalSymbol);
        }
    }

    private static HashSet<Field> filterByRelation(Set<Field> fields, DocTableRelation rel) {
        HashSet<Field> filteredFields = new HashSet<>();
        for (Field field : fields) {
            if (field.relation() == rel) {
                filteredFields.add(field);
            }
        }
        return filteredFields;
    }

    private void allocateFetchedReference(FetchReference fr, DocTableRelation rel) {
        FetchSource fs = fetchSources.get(fr.ref().ident().tableIdent());
        if (fs == null) {
            fs = new FetchSource(rel.tableInfo().partitionedByColumns());
            fetchSources.put(fr.ref().ident().tableIdent(), fs);
        }
        fs.fetchIdCols().add(fr.fetchId());
        if (fr.ref().granularity() == RowGranularity.DOC) {
            fs.references().add(fr.ref());
        }
    }
}
