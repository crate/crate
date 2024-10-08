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

package io.crate.expression.scalar.string;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.elasticsearch.common.hash.MessageDigests;

import io.crate.common.Hex;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public final class HashFunctions {

    public static void register(Functions.Builder builder) {
        register(builder, "md5", HashMethod.MD5::digest);
        register(builder, "sha1", HashMethod.SHA1::digest);
    }

    private static void register(Functions.Builder builder, String name, UnaryOperator<String> func) {
        builder.add(
            Signature.builder(name, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC, Scalar.Feature.STRICTNULL)
                .build(),
            (signature, boundSignature) ->
                new UnaryScalar<>(signature, boundSignature, DataTypes.STRING, func)
        );
    }

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

        public String digest(String input) {
            MessageDigest messageDigest = messageDigestSupplier.get();
            byte[] result = messageDigest.digest(input.getBytes(StandardCharsets.UTF_8));
            return Hex.encodeHexString(result);
        }
    }
}
