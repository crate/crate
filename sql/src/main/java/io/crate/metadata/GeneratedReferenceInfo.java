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

package io.crate.metadata;

import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.types.DataType;

import java.util.List;

public class GeneratedReferenceInfo extends ReferenceInfo {

    private final String formattedGeneratedExpression;
    private Symbol generatedExpression;
    private List<ReferenceInfo> referencedReferenceInfos;

    public GeneratedReferenceInfo(ReferenceIdent ident,
                                  RowGranularity granularity,
                                  DataType type,
                                  ColumnPolicy columnPolicy,
                                  IndexType indexType,
                                  String formattedGeneratedExpression) {
        super(ident, granularity, type, columnPolicy, indexType);
        this.formattedGeneratedExpression = formattedGeneratedExpression;
    }

    public GeneratedReferenceInfo(ReferenceIdent ident,
                                  RowGranularity granularity,
                                  DataType type,
                                  String formattedGeneratedExpression) {
        super(ident, granularity, type);
        this.formattedGeneratedExpression = formattedGeneratedExpression;
    }

    public String formattedGeneratedExpression() {
        return formattedGeneratedExpression;
    }

    public void generatedExpression(Symbol generatedExpression) {
        this.generatedExpression = generatedExpression;
    }

    public Symbol generatedExpression() {
        assert generatedExpression != null : "Generated expression symbol must not be NULL, initialize first";
        return generatedExpression;
    }

    public void referencedReferenceInfos(List<ReferenceInfo> referenceInfos) {
        this.referencedReferenceInfos = referenceInfos;
    }

    public List<ReferenceInfo> referencedReferenceInfos() {
        return referencedReferenceInfos;
    }
}
