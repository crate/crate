/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.expression.scalar.string;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.execution.expression.scalar.ScalarFunctionModule;
import io.crate.execution.expression.scalar.UnaryScalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MessageDigests;

import java.security.DigestException;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public final class HashFunctions {

    private static final List<DataType> SUPPORTED_INPUT_TYPE = Collections.singletonList((DataTypes.STRING));

    private enum HashMethod {
        MD5(MessageDigests::md5),
        SHA1(MessageDigests::sha1);

        /**
         * Do not pass the MessageDigest in directly but resolve it during
         * runtime to avoid concurrency issue when the hash function is
         * used by multiple threads at the same time.
         */
        private final Supplier<MessageDigest> messageDigestSupplier;

        HashMethod(Supplier<MessageDigest> digestSupplier) {
            this.messageDigestSupplier = digestSupplier;
        }

        public BytesRef digest(BytesRef input) {
            MessageDigest messageDigest = messageDigestSupplier.get();
            byte[] digest = new byte[messageDigest.getDigestLength()];
            messageDigest.update(input.bytes, input.offset, input.length);
            try {
                messageDigest.digest(digest, 0, digest.length);
            } catch (DigestException e) {
                throw new RuntimeException("Error computing digest.", e);
            }
            return new BytesRef(convertToHex(digest));
        }

        /**
         * Converts a byte array into an ASCII/UTF-8 hex encoded string.
         * @param input the input array to transform to a hex string
         * @return The resulting ASCII/UTF-8 encoded byte array which holds the hex string.
         */
        private static byte[] convertToHex(byte[] input) {
            byte[] hexString = new byte[input.length << 1];
            for (int temp, i = 0; i < input.length; i++) {
                temp = (input[i] >> 4) & 0x0F;
                hexString[2 * i] = (byte) (temp < 10 ? temp + '0' : temp + 'W');
                temp = input[i] & 0x0F;
                hexString[2 * i + 1] = (byte) (temp < 10 ? temp + '0' : temp + 'W');
            }
            return hexString;
        }
    }


    private static void register(ScalarFunctionModule module, String name, Function<BytesRef, BytesRef> func) {
        FunctionIdent ident = new FunctionIdent(name, SUPPORTED_INPUT_TYPE);
        module.register(new UnaryScalar<>(new FunctionInfo(ident, DataTypes.STRING), func));
    }

    public static void register(ScalarFunctionModule module) {
        register(module, "md5", HashMethod.MD5::digest);
        register(module, "sha1", HashMethod.SHA1::digest);
    }
}
