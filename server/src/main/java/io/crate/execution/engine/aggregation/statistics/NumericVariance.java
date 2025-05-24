/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.aggregation.statistics;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.types.NumericType;

public class NumericVariance implements Writeable, Comparable<NumericVariance> {

    private BigDecimal sumOfSqrs;
    private BigDecimal sum;
    private long count;

    public NumericVariance() {
        sumOfSqrs = new BigDecimal(0);
        sum = new BigDecimal(0);
        count = 0;
    }

    public NumericVariance(StreamInput in) throws IOException {
        sumOfSqrs = NumericType.INSTANCE.readValueFrom(in);
        sum = NumericType.INSTANCE.readValueFrom(in);
        count = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        NumericType.INSTANCE.writeValueTo(out, sumOfSqrs);
        NumericType.INSTANCE.writeValueTo(out, sum);
        out.writeVLong(count);
    }

    protected long count() {
        return count;
    }

    public void increment(BigDecimal value) {
        sumOfSqrs = sumOfSqrs.add(value.multiply(value));
        sum = sum.add(value);
        count++;
    }

    public void decrement(BigDecimal value) {
        sumOfSqrs = sumOfSqrs.subtract(value.multiply(value));
        sum = sum.subtract(value);
        count--;
    }

    public BigDecimal result() {
        if (count == 0) {
            return null;
        }
        return sumOfSqrs.subtract(sum.multiply(sum).divide(BigDecimal.valueOf(count), MathContext.DECIMAL128))
            .divide(BigDecimal.valueOf(count), MathContext.DECIMAL128);
    }

    public void merge(NumericVariance other) {
        sumOfSqrs = sumOfSqrs.add(other.sumOfSqrs);
        sum = sum.add(other.sum);
        count += other.count;
    }

    public long size() {
        return NumericType.size(sum) + NumericType.size(sumOfSqrs) + 64;
    }

    @Override
    public int compareTo(NumericVariance o) {
        return result().compareTo(o.result());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NumericVariance variance = (NumericVariance) o;
        return Objects.equals(variance.result(), result());
    }

    @Override
    public int hashCode() {
        return Objects.hash(result());
    }
}
