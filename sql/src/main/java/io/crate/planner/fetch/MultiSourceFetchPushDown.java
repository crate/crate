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
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.*;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;

import java.util.*;

public class MultiSourceFetchPushDown {

    private final MultiSourceSelect statement;

    private List<Symbol> remainingOutputs;
    private Map<TableIdent, FetchSource> fetchSources;


    public static MultiSourceFetchPushDown pushDown(MultiSourceSelect statement) {
        MultiSourceFetchPushDown pd = new MultiSourceFetchPushDown(statement);
        pd.process();
        return pd;
    }

    private MultiSourceFetchPushDown(MultiSourceSelect statement) {
        this.statement = statement;
        this.fetchSources = new HashMap<>(statement.sources().size());
    }

    public Map<TableIdent, FetchSource> fetchSources() {
        return fetchSources;
    }

    public List<Symbol> remainingOutputs() {
        return remainingOutputs;
    }

    private void process() {
        remainingOutputs = statement.querySpec().outputs();
        statement.querySpec().outputs(new ArrayList<Symbol>());

        HashMap<Symbol, Symbol> topLevelOutputMap = new HashMap<>(statement.canBeFetched().size());
        HashMap<Symbol, Symbol> mssOutputMap = new HashMap<>(statement.querySpec().outputs().size() + 2);

        ArrayList<Symbol> mssOutputs = new ArrayList<>(
                statement.sources().size() + statement.requiredForQuery().size());

        for (Map.Entry<QualifiedName, MultiSourceSelect.Source> entry : statement.sources().entrySet()) {
            MultiSourceSelect.Source source = entry.getValue();
            if (!(source.relation() instanceof DocTableRelation)) {
                continue;
            }

            DocTableRelation rel = (DocTableRelation) source.relation();
            HashSet<Field> canBeFetched = new HashSet<>();
            for (Field field : statement.canBeFetched()) {
                if (field.relation() == rel) {
                    canBeFetched.add(field);
                }
            }
            if (!canBeFetched.isEmpty()) {
                RelationColumn docIdColumn = new RelationColumn(entry.getKey(), 0, DataTypes.LONG);
                mssOutputs.add(docIdColumn);
                InputColumn docIdInput = new InputColumn(mssOutputs.size() - 1);

                ArrayList<Symbol> qtOutputs = new ArrayList<>(
                        source.querySpec().outputs().size() - canBeFetched.size() + 1);
                Reference docId = new Reference(((DocTableRelation) source.relation()).tableInfo().getReferenceInfo(DocSysColumns.DOCID));
                qtOutputs.add(docId);

                for (Symbol output : source.querySpec().outputs()) {
                    if (!canBeFetched.contains(output)) {
                        qtOutputs.add(output);
                        RelationColumn rc = new RelationColumn(entry.getKey(),
                                qtOutputs.size() - 1, output.valueType());
                        mssOutputs.add(rc);
                        mssOutputMap.put(output, rc);
                        topLevelOutputMap.put(output, new InputColumn(mssOutputs.size() - 1, output.valueType()));
                    }
                }
                for (Field field : canBeFetched) {
                    FetchReference fr = new FetchReference(
                            docIdInput, DocReferenceConverter.toSourceLookup(rel.resolveField(field)));
                    allocateFetchedReference(fr, rel);
                    topLevelOutputMap.put(field, fr);
                }
                source.querySpec().outputs(qtOutputs);
            } else {
                int index = 0;
                for (Symbol output : source.querySpec().outputs()) {
                    RelationColumn rc = new RelationColumn(entry.getKey(), index++, output.valueType());
                    mssOutputs.add(rc);
                    mssOutputMap.put(output, rc);
                    topLevelOutputMap.put(output, new InputColumn(mssOutputs.size() - 1, output.valueType()));
                }
            }
        }

        // replace the remaining order by symbols
        if (statement.remainingOrderBy().isPresent()) {
            for (Symbol symbol : statement.remainingOrderBy().get().orderBySymbols()) {
                if (!mssOutputMap.containsKey(symbol)) {
                    assert !mssOutputs.contains(symbol);
                    mssOutputs.add(symbol);
                    InputColumn inputColumn = new InputColumn(mssOutputs.size() - 1, symbol.valueType());
                    topLevelOutputMap.put(symbol, inputColumn);
                }
            }
        }

        statement.querySpec().outputs(mssOutputs);
        MappingSymbolVisitor.inPlace().processInplace(remainingOutputs, topLevelOutputMap);
        if (statement.querySpec().orderBy().isPresent()) {
            MappingSymbolVisitor.inPlace().processInplace(statement.querySpec().orderBy().get().orderBySymbols(), mssOutputMap);
        }
    }

    private void allocateFetchedReference(FetchReference fr, DocTableRelation rel) {
        FetchSource fs = fetchSources.get(fr.ref().ident().tableIdent());
        if (fs == null) {
            fs = new FetchSource(rel.tableInfo().partitionedByColumns());
            fetchSources.put(fr.ref().ident().tableIdent(), fs);
        }
        fs.docIdCols().add((InputColumn) fr.docId());
        if (fr.ref().info().granularity() == RowGranularity.DOC) {
            fs.references().add(fr.ref());
        }
    }
}

