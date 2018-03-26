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

import com.google.common.collect.Lists;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.RelationNormalizer;
import io.crate.analyze.relations.TableRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Functions;
import io.crate.metadata.Path;
import io.crate.metadata.TransactionContext;

import java.util.Collection;
import java.util.List;

class Relations {

    static Collection<? extends Path> namesFromOutputs(List<Symbol> outputs) {
        return Lists.transform(outputs, Symbols::pathFromSymbol);
    }

    /**
     * Promotes the relation to a QueriedRelation by applying the QuerySpec.
     *
     * <pre>
     * TableRelation -> QueriedTable
     * QueriedTable  -> QueriedSelect
     * QueriedSelect -> nested QueriedSelect
     * </pre>
     *
     * If the result is a QueriedTable it is also normalized.
     */
    static QueriedRelation applyQSToRelation(RelationNormalizer normalizer,
                                             Functions functions,
                                             TransactionContext transactionContext,
                                             AnalyzedRelation relation,
                                             QuerySpec querySpec) {
        QueriedRelation newRelation;
        if (relation instanceof DocTableRelation) {
            QueriedDocTable queriedDocTable = new QueriedDocTable((DocTableRelation) relation, querySpec);
            queriedDocTable.normalize(functions, transactionContext);
            newRelation = queriedDocTable;
        } else if (relation instanceof TableRelation) {
            QueriedTable queriedTable = new QueriedTable((TableRelation) relation, querySpec);
            queriedTable.normalize(functions, transactionContext);
            newRelation = queriedTable;
        } else {
            newRelation = new QueriedSelectRelation(
                ((QueriedRelation) relation), namesFromOutputs(querySpec.outputs()), querySpec);
            newRelation = (QueriedRelation) normalizer.normalize(newRelation, transactionContext);
        }
        newRelation.setQualifiedName(relation.getQualifiedName());
        return newRelation;
    }
}
