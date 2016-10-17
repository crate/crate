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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.primitives.Booleans;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.exceptions.AmbiguousOrderByException;
import io.crate.metadata.TransactionContext;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class OrderBy implements Streamable {

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

    private OrderBy() {
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

    public void normalize(EvaluatingNormalizer normalizer, TransactionContext transactionContext) {
        normalizer.normalizeInplace(orderBySymbols, transactionContext);
    }


    public OrderBy subset(Collection<Integer> positions) {
        List<Symbol> orderBySymbols = new ArrayList<>(positions.size());
        Boolean[] nullsFirst = new Boolean[positions.size()];
        boolean[] reverseFlags = new boolean[positions.size()];
        int pos = 0;
        for (Integer i : positions) {
            orderBySymbols.add(this.orderBySymbols.get(i));
            nullsFirst[pos] = this.nullsFirst[i];
            reverseFlags[pos] = this.reverseFlags[i];
            pos++;
        }
        return new OrderBy(orderBySymbols, reverseFlags, nullsFirst);
    }

    /**
     * Create a new OrderBy with symbols that match the predicate
     */
    @Nullable
    public OrderBy subset(Predicate<? super Symbol> predicate) {
        List<Integer> subSet = new ArrayList<>();
        Integer i = 0;
        for (Symbol orderBySymbol : orderBySymbols) {
            if (predicate.apply(orderBySymbol)) {
                subSet.add(i);
            }
            i++;
        }
        if (subSet.isEmpty()) {
            return null;
        }
        return subset(subSet);
    }

    public static void toStream(OrderBy orderBy, StreamOutput out) throws IOException {
        orderBy.writeTo(out);
    }

    public static OrderBy fromStream(StreamInput in) throws IOException {
        OrderBy orderBy = new OrderBy();
        orderBy.readFrom(in);
        return orderBy;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
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

    public OrderBy copyAndReplace(Function<? super Symbol, Symbol> replaceFunction) {
        return new OrderBy(Lists.newArrayList(Lists.transform(orderBySymbols, replaceFunction)), reverseFlags, nullsFirst);
    }

    public void replace(Function<? super Symbol, Symbol> replaceFunction) {
        ListIterator<Symbol> listIt = orderBySymbols.listIterator();
        while (listIt.hasNext()) {
            listIt.set(replaceFunction.apply(listIt.next()));
        }
    }

    public OrderBy merge(@Nullable OrderBy otherOrderBy) {
        if (otherOrderBy != null) {
            List<Symbol> newOrderBySymbols = new ArrayList<>(otherOrderBy.orderBySymbols());
            List<Boolean> newReverseFlags = new ArrayList<>(Booleans.asList(otherOrderBy.reverseFlags()));
            List<Boolean> newNullsFirst = new ArrayList<>(Arrays.asList(otherOrderBy.nullsFirst()));

            for (int i = 0; i < orderBySymbols.size(); i++) {
                Symbol orderBySymbol = orderBySymbols.get(i);
                int idx = newOrderBySymbols.indexOf(orderBySymbol);
                if (idx == -1) {
                    newOrderBySymbols.add(orderBySymbol);
                    newReverseFlags.add(reverseFlags[i]);
                    newNullsFirst.add(nullsFirst[i]);
                } else {
                    if (newReverseFlags.get(idx) != reverseFlags[i]) {
                        throw new AmbiguousOrderByException(orderBySymbol);
                    }
                    if (newNullsFirst.get(idx) != nullsFirst[i]) {
                        throw new AmbiguousOrderByException(orderBySymbol);
                    }
                }
            }

            this.orderBySymbols = newOrderBySymbols;
            this.reverseFlags = Booleans.toArray(newReverseFlags);
            this.nullsFirst = newNullsFirst.toArray(new Boolean[0]);
        }
        return this;
    }

    @Nullable
    public static OrderBy merge(com.google.common.base.Optional<OrderBy> o1, com.google.common.base.Optional<OrderBy> o2) {
        if (!o1.isPresent() || !o2.isPresent()) {
            return o1.or(o2).orNull();
        }
        return o1.get().merge(o2.get());
    }
}
