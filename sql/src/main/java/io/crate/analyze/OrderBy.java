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

import com.google.common.primitives.Booleans;
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
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <pre>
 *   ORDER BY { expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] } [, ...]
 *                                 ^^^^
 *                                 reverseFlag: true
 *
 *
 *   nullsFirst: [ null | true | false ]
 *                  |      ^       ^
 *                  |      |       |
 *                  +--  DESC      |      // null is re-written to true or false depending on ASC|DESC
 *                  |              |
 *                  +--   ASC -----+
 * </pre>
 */
public class OrderBy implements Writeable {

    private static final boolean REVERSE_FLAG_DEFAULT_ASC = false;
    private static final Boolean NULLS_FIRST_DEFAULT_FOR_ASC = false;

    private final List<Symbol> orderBySymbols;
    private final boolean[] reverseFlags;
    private final boolean[] nullsFirst;

    /**
     * Create a OrderBy with reverseFlags and nullsFirst defaults
     */
    public OrderBy(List<Symbol> orderBySymbols) {
        this.orderBySymbols = orderBySymbols;
        this.reverseFlags = new boolean[orderBySymbols.size()];
        this.nullsFirst = new boolean[orderBySymbols.size()];
        Arrays.fill(reverseFlags, REVERSE_FLAG_DEFAULT_ASC);
        Arrays.fill(nullsFirst, NULLS_FIRST_DEFAULT_FOR_ASC);
    }

    public OrderBy(List<Symbol> orderBySymbols, boolean[] reverseFlags, boolean[] nullsFirst) {
        assert !orderBySymbols.isEmpty() : "orderBySymbols must not be empty";
        assert orderBySymbols.size() == reverseFlags.length && reverseFlags.length == nullsFirst.length :
            "size of symbols / reverseFlags / nullsFirst must match";

        this.orderBySymbols = orderBySymbols;
        this.reverseFlags = reverseFlags;
        this.nullsFirst = nullsFirst;
    }

    /**
     * Creates a new OrderBy with the other symbols prepended (or returns this if symbols are empty).
     * Symbols are de-duplicated to some degree, e.g.
     *
     * <pre>
     *     ORDER BY x y `prependUnique` x z will result in x z y; without duplicating x
     * </pre>
     *
     * The defaults for reverseFlags and nullsFirst are used (asc, undefined)
     */
    public OrderBy prependUnique(Collection<? extends Symbol> symbols) {
        if (symbols.isEmpty()) {
            return this;
        }
        int newEstimatedSize = orderBySymbols.size() + symbols.size();
        ArrayList<Symbol> newOrderBySymbols = new ArrayList<>(newEstimatedSize);
        ArrayList<Boolean> newReverseFlags = new ArrayList<>(newEstimatedSize);
        ArrayList<Boolean> newNullsFirst = new ArrayList<>(newEstimatedSize);
        var orderBySymbols = this.orderBySymbols.listIterator();
        var xsToPrepend = symbols.iterator();
        var nextOrderBy = orderBySymbols.hasNext() ? orderBySymbols.next() : null;
        while (xsToPrepend.hasNext()) {
            Symbol toPrepend = xsToPrepend.next();
            if (toPrepend.equals(nextOrderBy)) {
                newOrderBySymbols.add(nextOrderBy);
                newReverseFlags.add(reverseFlags[orderBySymbols.previousIndex()]);
                newNullsFirst.add(nullsFirst[orderBySymbols.previousIndex()]);
                nextOrderBy = orderBySymbols.hasNext() ? orderBySymbols.next() : null;
            } else {
                newOrderBySymbols.add(toPrepend);
                newReverseFlags.add(REVERSE_FLAG_DEFAULT_ASC);
                newNullsFirst.add(NULLS_FIRST_DEFAULT_FOR_ASC);
            }
        }
        if (nextOrderBy != null) {
            newOrderBySymbols.add(nextOrderBy);
            newReverseFlags.add(reverseFlags[orderBySymbols.previousIndex()]);
            newNullsFirst.add(nullsFirst[orderBySymbols.previousIndex()]);
        }
        while (orderBySymbols.hasNext()) {
            newOrderBySymbols.add(orderBySymbols.next());
            newReverseFlags.add(reverseFlags[orderBySymbols.previousIndex()]);
            newNullsFirst.add(nullsFirst[orderBySymbols.previousIndex()]);
        }
        return new OrderBy(newOrderBySymbols, Booleans.toArray(newReverseFlags), Booleans.toArray(newNullsFirst));
    }

    public List<Symbol> orderBySymbols() {
        return orderBySymbols;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public boolean[] nullsFirst() {
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
        nullsFirst = new boolean[numOrderBy];
        for (int i = 0; i < numOrderBy; i++) {
            nullsFirst[i] = in.readBoolean();
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
        for (boolean nullFirst : nullsFirst) {
            out.writeBoolean(nullFirst);
        }
    }

    public OrderBy copyAndReplace(Function<? super Symbol, ? extends Symbol> replaceFunction) {
        return new OrderBy(Lists2.map(orderBySymbols, replaceFunction), reverseFlags, nullsFirst);
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
        return Objects.hash(orderBySymbols) + Arrays.deepHashCode(new Object[]{reverseFlags, nullsFirst});
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
                                                      boolean[] nullsFirst) {
        for (int i = 0; i < leaves.size(); i++) {
            ExplainLeaf leaf = leaves.get(i);
            sb.append(leaf.representation());
            sb.append(" ");
            if (reverseFlags[i]) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
            boolean nullFirst = nullsFirst[i];
            if (reverseFlags[i] != nullFirst) {
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
