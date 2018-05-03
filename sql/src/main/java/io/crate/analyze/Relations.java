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
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.RelationNormalizer;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Functions;
import io.crate.metadata.Path;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.sql.tree.QualifiedName;

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
        if (relation instanceof AbstractTableRelation) {
            AbstractTableRelation<?> tableRelation = (AbstractTableRelation<?>) relation;
            EvaluatingNormalizer evalNormalizer = new EvaluatingNormalizer(
                functions, RowGranularity.CLUSTER, null, tableRelation);

            newRelation = new QueriedTable<>(
                tableRelation, querySpec.copyAndReplace(s -> evalNormalizer.normalize(s, transactionContext)));
        } else {
            newRelation = new QueriedSelectRelation(
                ((QueriedRelation) relation), namesFromOutputs(querySpec.outputs()), querySpec);
            newRelation = (QueriedRelation) normalizer.normalize(newRelation, transactionContext);
        }
        if (newRelation.where().hasQuery() && newRelation.getQualifiedName().equals(relation.getQualifiedName())) {
            // This relation will be represented as a subquery and needs a proper QualifiedName.
            // If there is no distinct QualifiedName, create one by merging all parts of the old QualifiedName
            // e.g. "doc"."users" -> "doc.users"
            newRelation.setQualifiedName(QualifiedName.of(relation.getQualifiedName().toString()));
        } else {
            newRelation.setQualifiedName(relation.getQualifiedName());
        }
        return newRelation;
    }
}
