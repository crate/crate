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
import java.util.function.UnaryOperator;

import org.apache.commons.codec.digest.Blake3;
import org.elasticsearch.common.hash.MessageDigests;

import io.crate.common.Hex;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.Signature.Feature;
import io.crate.types.DataTypes;

public final class HashFunctions {

    public static void register(Functions.Builder builder) {
        register(builder, "md5", HashMethod.MD5::digest);
        register(builder, "sha1", HashMethod.SHA1::digest);
        register(builder, "blake3", HashMethod.BLAKE3::digest);
    }

    private static void register(Functions.Builder builder, String name, UnaryOperator<String> func) {
        builder.add(
            Signature.builder(name, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            (signature, boundSignature) ->
                new UnaryScalar<>(signature, boundSignature, func)
        );
    }

    private enum HashMethod {
        MD5(bytes -> MessageDigests.md5().digest(bytes)),
        SHA1(bytes -> MessageDigests.sha1().digest(bytes)),
        BLAKE3(bytes -> {
            Blake3 digest = Blake3.initHash();
            digest.update(bytes);
            // Blake3 defaults to 32 byte (256-bit) output
            return digest.doFinalize(32);
        });

        private final UnaryOperator<byte[]> byteHasher;

        HashMethod(UnaryOperator<byte[]> byteHasher) {
            this.byteHasher = byteHasher;
        }

        public String digest(String input) {
            return Hex.encodeHexString(byteHasher.apply(input.getBytes(StandardCharsets.UTF_8)));
        }
    }
}
