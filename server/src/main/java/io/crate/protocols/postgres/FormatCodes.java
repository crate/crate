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

package io.crate.protocols.postgres;


import io.netty.buffer.ByteBuf;

import org.jspecify.annotations.Nullable;

public class FormatCodes {

    public enum FormatCode {
        TEXT, // 0
        BINARY // 1
    }

    private static final FormatCode[] EMPTY_FORMAT_CODES = new FormatCode[0];

    /**
     * Read format codes from a ByteBuf.
     * <p>
     * Buffer must contain:
     * <pre>
     * int16 num formatCodes
     *      foreach:
     *      int16 formatCode
     * </pre>
     */
    static FormatCode[] fromBuffer(ByteBuf buffer) {
        short numFormatCodes = buffer.readShort();
        if (numFormatCodes == 0) {
            return EMPTY_FORMAT_CODES;
        }
        FormatCode[] formatCodes = new FormatCode[numFormatCodes];
        for (int i = 0; i < numFormatCodes; i++) {
            formatCodes[i] = FormatCode.values()[buffer.readShort()];
        }
        return formatCodes;
    }

    /**
     * Get the formatCode for a column idx
     * <p>
     * According to spec:
     * length of formatCodes:
     * 0 = uses default (TEXT)
     * 1 = all params uses this format
     * n = one for each param
     */
    static FormatCode getFormatCode(@Nullable FormatCode[] formatCodes, int idx) {
        if (formatCodes == null || formatCodes.length == 0) {
            return FormatCode.TEXT;
        }
        return formatCodes.length == 1 ? formatCodes[0] : formatCodes[idx];
    }
}
