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

package io.crate.analyze;

import io.crate.collections.Lists2;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.ExplainLeaf;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public class OrderBy implements Writeable {

    private List<Symbol> orderBySymbols;
    private boolean[] reverseFlags;
    private Boolean[] nullsFirst;

    public OrderBy(List<Symbol> orderBySymbols, boolean[] reverseFlags, Boolean[] nullsFirst) {
        assert !orderBySymbols.isEmpty() : "orderBySymbols must not be empty";
        assert orderBySymbols.size() == reverseFlags.length && reverseFlags.length == nullsFirst.length :
            "size of symbols / reverseFlags / nullsFirst must match";

        this.orderBySymbols = orderBySymbols;
        this.reverseFlags = reverseFlags;
        this.nullsFirst = nullsFirst;
    }

    public List<Symbol> orderBySymbols() {
        return orderBySymbols;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public Boolean[] nullsFirst() {
        return nullsFirst;
    }

    public OrderBy(StreamInput in) throws IOException {
        int numOrderBy = in.readVInt();
        reverseFlags = new boolean[numOrderBy];

        for (int i = 0; i < numOrderBy; i++) {
            reverseFlags[i] = in.readBoolean();
        }

        orderBySymbols = new ArrayList<>(numOrderBy);
        for (int i = 0; i < numOrderBy; i++) {
            orderBySymbols.add(Symbols.fromStream(in));
        }

        nullsFirst = new Boolean[numOrderBy];
        for (int i = 0; i < numOrderBy; i++) {
            nullsFirst[i] = in.readOptionalBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(reverseFlags.length);
        for (boolean reverseFlag : reverseFlags) {
            out.writeBoolean(reverseFlag);
        }
        for (Symbol symbol : orderBySymbols) {
            Symbols.toStream(symbol, out);
        }
        for (Boolean nullFirst : nullsFirst) {
            out.writeOptionalBoolean(nullFirst);
        }
    }

    public OrderBy copyAndReplace(Function<? super Symbol, ? extends Symbol> replaceFunction) {
        return new OrderBy(Lists2.copyAndReplace(orderBySymbols, replaceFunction), reverseFlags, nullsFirst);
    }

    public void replace(Function<? super Symbol, ? extends Symbol> replaceFunction) {
        ListIterator<Symbol> listIt = orderBySymbols.listIterator();
        while (listIt.hasNext()) {
            listIt.set(replaceFunction.apply(listIt.next()));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderBy orderBy = (OrderBy) o;
        return orderBySymbols.equals(orderBy.orderBySymbols) &&
               Arrays.equals(reverseFlags, orderBy.reverseFlags) &&
               Arrays.equals(nullsFirst, orderBy.nullsFirst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderBySymbols, reverseFlags, nullsFirst);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OrderBy{");
        explainRepresentation(sb, orderBySymbols, reverseFlags, nullsFirst);
        return sb.toString();
    }

    public static StringBuilder explainRepresentation(StringBuilder sb,
                                                      List<? extends ExplainLeaf> leaves,
                                                      boolean[] reverseFlags,
                                                      Boolean[] nullsFirst) {
        for (int i = 0; i < leaves.size(); i++) {
            ExplainLeaf leaf = leaves.get(i);
            sb.append(leaf.representation());
            sb.append(" ");
            if (reverseFlags[i]) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
            Boolean nullFirst = nullsFirst[i];
            if (nullFirst != null) {
                sb.append(" ");
                sb.append(nullFirst ? "NULLS FIRST" : "NULLS LAST");
            }
            if (i + 1 < leaves.size()) {
                sb.append(" ");
            }
        }
        return sb;
    }

    public void accept(Consumer<? super Symbol> consumer) {
        for (Symbol sortItem : orderBySymbols) {
            consumer.accept(sortItem);
        }
    }
}
