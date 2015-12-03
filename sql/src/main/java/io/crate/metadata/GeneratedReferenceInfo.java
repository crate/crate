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

import com.google.common.base.Objects;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GeneratedReferenceInfo extends ReferenceInfo {

    public static final ReferenceInfoFactory<GeneratedReferenceInfo> FACTORY = new ReferenceInfoFactory<GeneratedReferenceInfo>() {
        @Override
        public GeneratedReferenceInfo newInstance() {
            return new GeneratedReferenceInfo();
        }
    };

    private String formattedGeneratedExpression;
    private Symbol generatedExpression;
    private List<ReferenceInfo> referencedReferenceInfos;

    private GeneratedReferenceInfo() {
    }

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

    @Override
    public ReferenceInfoType referenceInfoType() {
        return ReferenceInfoType.GENERATED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GeneratedReferenceInfo that = (GeneratedReferenceInfo) o;
        return Objects.equal(formattedGeneratedExpression, that.formattedGeneratedExpression) &&
               Objects.equal(generatedExpression, that.generatedExpression) &&
               Objects.equal(referencedReferenceInfos, that.referencedReferenceInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), formattedGeneratedExpression,
                generatedExpression, referencedReferenceInfos);
    }

    @Override
    public String toString() {
        return "GeneratedReferenceInfo{" +
               "formattedGeneratedExpression='" + formattedGeneratedExpression + '\'' +
               ", generatedExpression=" + generatedExpression +
               ", referencedReferenceInfos=" + referencedReferenceInfos +
               '}';
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        formattedGeneratedExpression = in.readString();
        generatedExpression = Symbol.fromStream(in);
        int size = in.readVInt();
        referencedReferenceInfos = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            referencedReferenceInfos.add(ReferenceInfo.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(formattedGeneratedExpression);
        Symbol.toStream(generatedExpression, out);
        out.writeVInt(referencedReferenceInfos.size());
        for (ReferenceInfo referenceInfo : referencedReferenceInfos) {
            ReferenceInfo.toStream(referenceInfo, out);
        }
    }
}