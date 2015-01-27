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

package io.crate.planner.symbol;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class Reference extends Symbol {

    public static final SymbolFactory<Reference> FACTORY = new SymbolFactory<Reference>() {
        @Override
        public Reference newInstance() {
            return new Reference();
        }
    };

    protected ReferenceInfo info;

    public Reference(ReferenceInfo info) {
        Preconditions.checkArgument(info!=null, "Info is null");
        this.info = info;
    }

    public Reference() {

    }

    public ReferenceInfo info() {
        return info;
    }

    public ReferenceIdent ident() {
        return info.ident();
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.REFERENCE;
    }

    @Override
    public DataType valueType() {
        return info().type();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("info", info())
                .toString();
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitReference(this, context);
    }

    public static Reference fromStream(StreamInput in) throws IOException {
        Reference reference = new Reference();
        reference.readFrom(in);
        return reference;
    }

    public static void toStream(Reference reference, StreamOutput out) throws IOException {
        reference.writeTo(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        info = new ReferenceInfo();
        info.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        info().writeTo(out);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        Reference o = (Reference) obj;
        return Objects.equal(info().ident(), o.info().ident());
    }

    @Override
    public int hashCode() {
        return info().hashCode();
    }
}
