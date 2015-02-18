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

package io.crate.planner.node.dql;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.analyze.Id;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Symbols;
import org.elasticsearch.common.Nullable;

import java.util.List;


public class ESGetNode extends ESDQLPlanNode implements DQLPlanNode {

    private final String index;
    private final List<String> ids;
    private final List<String> routingValues;
    private final List<Symbol> sortSymbols;
    private final boolean[] reverseFlags;
    private final Boolean[] nullsFirst;
    private final Integer limit;
    private final int offset;
    private final List<ReferenceInfo> partitionBy;
    private final boolean extractBytesRef;

    private final static boolean[] EMPTY_REVERSE_FLAGS = new boolean[0];
    private final static Boolean[] EMPTY_NULLS_FIRST = new Boolean[0];

    public ESGetNode(String index,
                     List<Symbol> outputs,
                     List<Id> ids,
                     @Nullable List<Symbol> sortSymbols,
                     @Nullable boolean[] reverseFlags,
                     @Nullable Boolean[] nullsFirst,
                     @Nullable Integer limit,
                     int offset,
                     @Nullable List<ReferenceInfo> partitionBy,
                     boolean extractBytesRef) {
        this.index = index;
        this.outputs = outputs;
        outputTypes(Symbols.extractTypes(outputs));
        this.ids = Lists.transform(ids, Id.ID_STRING_FUNCTION);
        this.routingValues = Lists.transform(ids, Id.ROUTING_VALUES_FUNCTION);
        this.sortSymbols = MoreObjects.firstNonNull(sortSymbols, ImmutableList.<Symbol>of());
        this.reverseFlags = MoreObjects.firstNonNull(reverseFlags, EMPTY_REVERSE_FLAGS);
        this.nullsFirst = MoreObjects.firstNonNull(nullsFirst, EMPTY_NULLS_FIRST);
        this.limit = limit;
        this.offset = offset;
        this.partitionBy = MoreObjects.firstNonNull(partitionBy, ImmutableList.<ReferenceInfo>of());
        this.extractBytesRef = extractBytesRef;
    }

    public String index() {
        return index;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitESGetNode(this, context);
    }

    public List<String> ids() {
        return ids;
    }

    public List<String> routingValues() {
        return routingValues;
    }

    @Nullable
    public Integer limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    public List<Symbol> sortSymbols() {
        return sortSymbols;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public Boolean[] nullsFirst() {
        return nullsFirst;
    }


    public List<ReferenceInfo> partitionBy() {
        return partitionBy;
    }

    public boolean extractBytesRef() {
        return extractBytesRef;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("index", index)
                .add("ids", ids)
                .add("outputs", outputs)
                .add("partitionBy", partitionBy)
                .toString();
    }
}
