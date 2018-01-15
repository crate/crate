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

package io.crate.execution.engine.aggregation.statistics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class Variance implements Streamable {

    double sumOfSqrs;
    double sum;
    long count;

    public Variance() {
        sumOfSqrs = 0.0;
        sum = 0.0;
        count = 0;
    }

    public void increment(double value) {
        sumOfSqrs += (value * value);
        sum += value;
        count++;
    }

    public synchronized double result() {
        if (count == 0) {
            return Double.NaN;
        }
        return (sumOfSqrs - ((sum * sum) / count)) / count;
    }

    public void merge(Variance other) {
        sumOfSqrs += other.sumOfSqrs;
        sum += other.sum;
        count += other.count;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        sumOfSqrs = in.readDouble();
        sum = in.readDouble();
        count = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(sumOfSqrs);
        out.writeDouble(sum);
        out.writeVLong(count);
    }
}
