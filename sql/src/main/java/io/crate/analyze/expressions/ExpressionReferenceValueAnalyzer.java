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

package io.crate.analyze.expressions;

import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataType;
import io.crate.types.ObjectType;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class ExpressionReferenceValueAnalyzer extends ExpressionAnalyzer {

    private static final ESLogger LOGGER = Loggers.getLogger(ExpressionReferenceValueAnalyzer.class);

    private final ReferenceValueResolver referenceValueResolver;

    public ExpressionReferenceValueAnalyzer(TableInfo tableInfo,
                                            List<Reference> references,
                                            AnalysisMetaData analysisMetaData,
                                            ParameterContext parameterContext) {
        super(analysisMetaData, parameterContext, null, null);
        this.innerAnalyzer = new InnerReferenceExpressionAnalyzer();
        this.referenceValueResolver = new ReferenceValueResolver(tableInfo, references);
    }

    public void values(Object[] values) {
        referenceValueResolver.values = values;
    }

    class InnerReferenceExpressionAnalyzer extends ExpressionAnalyzer.InnerExpressionAnalyzer {

        @Override
        protected Symbol resolveQualifiedName(QualifiedName qualifiedName, @Nullable List<String> path) {
            assert referenceValueResolver.values != null : "values must be set";
            return referenceValueResolver.resolveReferenceValue(qualifiedName, path);
        }
    }

    static class ReferenceValueResolver {

        private final TableInfo tableInfo;
        private final List<Reference> references;
        private Object[] values;

        public ReferenceValueResolver(TableInfo tableInfo, List<Reference> references) {
            this.tableInfo = tableInfo;
            this.references = references;
        }

        public Symbol resolveReferenceValue(QualifiedName qualifiedName, @Nullable List<String> path) {
            List<String> parts = qualifiedName.getParts();
            ColumnIdent columnIdent = new ColumnIdent(parts.get(parts.size() - 1), path);

            for (int i = 0; i < references.size(); i++) {
                Reference reference = references.get(i);
                DataType dataType = null;
                Object value = null;
                if (reference.ident().columnIdent().equals(columnIdent)) {
                    dataType = reference.valueType();
                    value = values[i];
                } else if (reference.valueType().id() == ObjectType.ID
                           && path != null
                           && reference.ident().columnIdent().name().equals(columnIdent.name())) {
                    ReferenceInfo referenceInfo = tableInfo.getReferenceInfo(columnIdent);
                    dataType = referenceInfo.type();
                    try {
                        int idx = 0;
                        value = ((Map) values[i]).get(path.get(idx++));
                        while (idx < path.size()) {
                            value = ((Map) value).get(path.get(idx));
                        }
                    } catch (Exception e) {
                        LOGGER.error("Exception occurred while resolving referenced column value", e);
                        break;
                    }
                }

                if (dataType != null) {
                    return Literal.newLiteral(dataType, dataType.value(value));
                }
            }

            // reference not found in statement, resolve it from table relation to get the right data type
            ReferenceInfo referenceInfo = tableInfo.getReferenceInfo(columnIdent);
            if (referenceInfo == null) {
                // should never happen
                throw new ColumnUnknownException(columnIdent.sqlFqn());
            }

            DataType dataType = referenceInfo.type();
            return Literal.newLiteral(dataType, dataType.value(null));
        }
    }
}
