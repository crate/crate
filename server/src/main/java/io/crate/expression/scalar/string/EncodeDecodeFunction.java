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

package io.crate.expression.scalar.string;

import io.crate.common.Hex;
import io.crate.common.Octal;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.scalar.arithmetic.BinaryScalar;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class EncodeDecodeFunction {

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                "encode",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, args) ->
                new BinaryScalar<>(
                    new Encode(),
                    "encode",
                    signature,
                    DataTypes.STRING,
                    FunctionInfo.DETERMINISTIC_ONLY
                )
        );
        module.register(
            Signature.scalar(
                "decode",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, args) ->
                new BinaryScalar<>(
                    new Decode(),
                    "decode",
                    signature,
                    DataTypes.STRING,
                    FunctionInfo.DETERMINISTIC_ONLY
                )
        );
    }

    /**
     * Takes a binary data of "bytea" type and encodes it into the given output format.
     */
    private static class Encode implements BinaryOperator<String> {

        @Override
        public String apply(String bytea, String format) {
            final Format fmt;

            try {
                fmt = Format.valueOf(format.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Encoding format '%s' is not supported", format)
                );
            }
            return fmt.encode(bytea);
        }

    }

    /**
     * Takes a string encoded into the given format and returns its hexadecimal representation.
     */
    private static class Decode implements BinaryOperator<String> {

        @Override
        public String apply(String text, String format) {
            final Format fmt;

            try {
                fmt = Format.valueOf(format.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Encoding format '%s' is not supported", format)
                );
            }
            return fmt.decode(text);
        }

    }

    /**
     * Supported encoding formats.
     */
    private enum Format {
        /**
         * Represent binary data using base64 encoding.
         */
        BASE64(bytea -> {
            final byte[] text;
            if (Hex.isHexFormat(bytea)) {
                text = Hex.decodeHex(Hex.stripHexFormatFlag(bytea));
            } else {
                text = Octal.decode(bytea);
            }
            return Base64.getEncoder().encodeToString(text);
        }, text -> {
            final byte[] value = Base64.getDecoder().decode(text.getBytes(StandardCharsets.UTF_8));
            return Hex.HEX_FLAG + Hex.encodeHexString(value);
        }),
        /**
         * Hexadecimal representation of binary data.
         */
        HEX(bytea -> {
            if (Hex.isHexFormat(bytea)) {
                // the input is already in hex format
                return Hex.stripHexFormatFlag(bytea);
            }
            return Hex.encodeHexString(Octal.decode(bytea));
        }, text -> Hex.HEX_FLAG + Hex.validateHex(text, 0)
        ),
        /**
         * Unprintable characters in binary data are written as octal numbers.
         */
        ESCAPE(bytea -> {
            if (Hex.isHexFormat(bytea)) {
                return Octal.encode(Hex.decodeHex(Hex.stripHexFormatFlag(bytea)));
            }
            return Octal.encode(bytea.getBytes(StandardCharsets.UTF_8));
        }, text -> Hex.HEX_FLAG + Hex.encodeHexString(Octal.decode(text)));

        private final Function<String, String> encode;
        private final Function<String, String> decode;

        Format(Function<String, String> encode, Function<String, String> decode) {
            this.encode = encode;
            this.decode = decode;
        }

        String encode(String bytea) {
            return encode.apply(bytea);
        }

        String decode(String text) {
            return decode.apply(text);
        }

    }

}
