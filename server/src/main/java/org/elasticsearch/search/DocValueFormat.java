/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Base64;

/** A formatter for values as returned by the fielddata/doc-values APIs. */
public interface DocValueFormat extends NamedWriteable {

    /** Format a long value. This is used by terms and histogram aggregations
     *  to format keys for fields that use longs as a doc value representation
     *  such as the {@code long} and {@code date} fields. */
    Object format(long value);

    /** Format a double value. This is used by terms and stats aggregations
     *  to format keys for fields that use numbers as a doc value representation
     *  such as the {@code long}, {@code double} or {@code date} fields. */
    Object format(double value);

    /** Format a binary value. This is used by terms aggregations to format
     *  keys for fields that use binary doc value representations such as the
     *  {@code keyword} and {@code ip} fields. */
    Object format(BytesRef value);

    DocValueFormat RAW = new DocValueFormat() {

        @Override
        public String getWriteableName() {
            return "raw";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }

        @Override
        public Long format(long value) {
            return value;
        }

        @Override
        public Double format(double value) {
            return value;
        }

        @Override
        public String format(BytesRef value) {
            return value.utf8ToString();
        }
    };

    DocValueFormat BINARY = new DocValueFormat() {

        @Override
        public String getWriteableName() {
            return "binary";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }

        @Override
        public Object format(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object format(double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String format(BytesRef value) {
            return Base64.getEncoder()
                    .withoutPadding()
                    .encodeToString(Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length));
        }
    };

    DocValueFormat BOOLEAN = new DocValueFormat() {

        @Override
        public String getWriteableName() {
            return "bool";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }

        @Override
        public Boolean format(long value) {
            return java.lang.Boolean.valueOf(value != 0);
        }

        @Override
        public Boolean format(double value) {
            return java.lang.Boolean.valueOf(value != 0);
        }

        @Override
        public String format(BytesRef value) {
            throw new UnsupportedOperationException();
        }
    };

    DocValueFormat IP = new DocValueFormat() {

        @Override
        public String getWriteableName() {
            return "ip";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }

        @Override
        public String format(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String format(double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String format(BytesRef value) {
            byte[] bytes = Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
            InetAddress inet = InetAddressPoint.decode(bytes);
            return NetworkAddress.format(inet);
        }
    };
}
