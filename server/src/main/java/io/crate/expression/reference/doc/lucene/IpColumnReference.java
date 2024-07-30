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

package io.crate.expression.reference.doc.lucene;


import java.net.InetAddress;
import java.util.Arrays;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.NetworkAddress;

public class IpColumnReference extends BinaryColumnReference<String> {

    public IpColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    protected String convert(BytesRef input) {
        return format(input);
    }

    /**
     * Formats a byte-encoded IP address as a String
     */
    public static String format(BytesRef value) {
        byte[] bytes = Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
        InetAddress inet = InetAddressPoint.decode(bytes);
        return NetworkAddress.format(inet);
    }
}
