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

package io.crate.protocols.http;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Arrays;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.quic.Quic;
import io.netty.handler.codec.quic.QuicTokenHandler;

/**
 * <p>Tokens are HMAC-SHA256 signed over (client IP, client port, destination connection ID,
 * expiry timestamp). This prevents IP-spoofing amplification attacks: a client must prove
 * it controls the source address before the server sends a large response.</p>
 *
 * <p>Token layout:
 * <pre>
 *   [0..7]              expiry epoch-second (long, big-endian)
 *   [8..39]             HMAC-SHA256 over (address bytes | port bytes | dcid bytes | expiry bytes)
 *   [40..40+dcidLen)    destination connection ID (dcid), variable length, 0–20 bytes
 * </pre>
 * </p>
 *
 * <p>The dcid is appended to the token so that {@link #validateToken} can recover it and return
 * its start index. netty-quic reads the bytes after that index as the
 * {@code original_destination_connection_id} QUIC transport parameter, see RFC 9000 §7.3.</p>
 */
public class HmacQuicTokenHandler implements QuicTokenHandler {

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final int EXPIRY_BYTES = Long.BYTES;        // 8
    private static final int HMAC_BYTES = 32;                   // SHA-256 output
    static final int FIXED_TOKEN_LENGTH = EXPIRY_BYTES + HMAC_BYTES; // 40

    private final byte[] secret;
    private final long validitySeconds;

    /**
     * @param secret          server-side secret used for HMAC signing; must be kept confidential
     *                        and rotated periodically
     * @param validitySeconds how long a token remains valid after issuance
     */
    public HmacQuicTokenHandler(byte[] secret, long validitySeconds) {
        if (secret.length < 16) {
            throw new IllegalArgumentException("QUIC token secret must be at least 16 bytes");
        }
        this.secret = Arrays.copyOf(secret, secret.length);
        this.validitySeconds = validitySeconds;
    }

    @Override
    public boolean writeToken(ByteBuf out, ByteBuf dcid, InetSocketAddress address) {
        try {
            long expiry = Instant.now().getEpochSecond() + validitySeconds;
            byte[] dcidBytes = new byte[dcid.readableBytes()];
            dcid.getBytes(dcid.readerIndex(), dcidBytes);
            byte[] hmac = computeHmac(address, dcidBytes, expiry);
            out.writeLong(expiry);
            out.writeBytes(hmac);
            // Append dcid so validateToken() can recover it and return its start offset.
            // netty-quic uses the bytes after that offset as original_destination_connection_id.
            out.writeBytes(dcidBytes);
            return true;
        } catch (InvalidKeyException | NoSuchAlgorithmException e) {
            return false;
        }
    }

    @Override
    public int maxTokenLength() {
        return FIXED_TOKEN_LENGTH + Quic.MAX_CONN_ID_LEN;
    }

    /**
     * Validates the token and returns the start index of the dcid within the token buffer,
     * or -1 if the token is invalid.
     *
     * <p>The return value is used by netty-quic to read {@code original_destination_connection_id}
     * from {@code token[returnValue..]}.</p>
     */
    @Override
    public int validateToken(ByteBuf token, InetSocketAddress address) {
        if (token.readableBytes() < FIXED_TOKEN_LENGTH) {
            return -1;
        }
        long expiry = token.getLong(token.readerIndex());
        if (Instant.now().getEpochSecond() > expiry) {
            return -1;
        }
        // Recover the dcid from the tail of the token (bytes after the fixed header).
        int dcidLen = token.readableBytes() - FIXED_TOKEN_LENGTH;
        byte[] dcidBytes = new byte[dcidLen];
        token.getBytes(token.readerIndex() + FIXED_TOKEN_LENGTH, dcidBytes);

        try {
            byte[] expected = computeHmac(address, dcidBytes, expiry);
            byte[] actual = new byte[HMAC_BYTES];
            token.getBytes(token.readerIndex() + EXPIRY_BYTES, actual);
            if (!MessageDigest.isEqual(expected, actual)) {
                return -1;
            }
            // Return the start offset of the dcid so netty-quic can recover
            // original_destination_connection_id from the token.
            return FIXED_TOKEN_LENGTH;
        } catch (InvalidKeyException | NoSuchAlgorithmException e) {
            return -1;
        }
    }

    private byte[] computeHmac(InetSocketAddress address, byte[] dcid, long expiry)
            throws NoSuchAlgorithmException, InvalidKeyException {
        Mac mac = Mac.getInstance(HMAC_ALGORITHM);
        mac.init(new SecretKeySpec(secret, HMAC_ALGORITHM));
        mac.update(address.getAddress().getAddress());
        mac.update(ByteBuffer.allocate(Integer.BYTES).putInt(address.getPort()).array());
        mac.update(dcid);
        mac.update(ByteBuffer.allocate(Long.BYTES).putLong(expiry).array());
        return mac.doFinal();
    }
}
