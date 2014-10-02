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

package io.crate.planner.symbol;

import com.google.common.base.Objects;
import io.crate.metadata.ReferenceInfo;

public class ReferencePlaceHolder<ReturnType> extends Literal<ReturnType> {

    public static SymbolFactory<ReferencePlaceHolder> FACTORY = new SymbolFactory<ReferencePlaceHolder>() {

        @Override
        public ReferencePlaceHolder newInstance() {
            return new ReferencePlaceHolder();
        }
    };

    private ReferencePlaceHolder() {
    }

    public ReferencePlaceHolder(ReferenceInfo info) {
        super(info.type(), null);
    }

    public void setValue(Object newValue) {
        if (newValue == null) {
            this.value = null;
        } else {
            this.value = type.value(newValue);
        }
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.REFERENCE_PLACEHOLDER;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitReferencePlaceHolder(this, context);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("type", type.getName())
                .add("value", value())
                .toString();
    }
}
