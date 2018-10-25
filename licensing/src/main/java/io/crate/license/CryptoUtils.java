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

package io.crate.license;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

final class CryptoUtils {

    static final String RSA_CIPHER_ALGORITHM = "RSA";
    private static final int KEY_SIZE = 2048;
    private static final String PASSPHRASE = "crate_passphrase";
    private static final String AES_CIPHER_ALGORITHM = "AES";
    private static final Key AES_KEY_SPEC = new SecretKeySpec(PASSPHRASE.getBytes(StandardCharsets.UTF_8), AES_CIPHER_ALGORITHM);

    private CryptoUtils() {
    }

    static KeyPair generateRSAKeyPair() {
        try {
            SecureRandom random = new SecureRandom();
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance(RSA_CIPHER_ALGORITHM);
            keyGen.initialize(KEY_SIZE, random);
            return keyGen.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] getPublicKeyBytes(PublicKey publicKey) {
        X509EncodedKeySpec encodedKeySpec = new X509EncodedKeySpec(publicKey.getEncoded());
        return encodedKeySpec.getEncoded();
    }

    static byte[] getPrivateKeyBytes(PrivateKey privateKey) {
        PKCS8EncodedKeySpec encodedKeySpec = new PKCS8EncodedKeySpec(privateKey.getEncoded());
        return encodedKeySpec.getEncoded();
    }

    static byte[] encryptAES(byte[] data) {
        return crypto(AES_CIPHER_ALGORITHM,
            Cipher.ENCRYPT_MODE,
            AES_KEY_SPEC,
            data);
    }

    static byte[] decryptAES(byte[] data) {
        return crypto(AES_CIPHER_ALGORITHM,
            Cipher.DECRYPT_MODE,
            AES_KEY_SPEC,
            data);
    }

    static byte[] decryptRSAUsingPublicKey(byte[] data, byte[] publicKeyBytes) {
        return crypto(RSA_CIPHER_ALGORITHM,
            Cipher.DECRYPT_MODE,
            getPublicKey(publicKeyBytes),
            data);
    }

    private static PublicKey getPublicKey(byte[] publicKeyBytes) {
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKeyBytes);
        try {
            return KeyFactory.getInstance(RSA_CIPHER_ALGORITHM).generatePublic(publicKeySpec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException(e);
        }
    }

    static PrivateKey getPrivateKey(byte[] privateKeyBytes) {
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
        try {
            return KeyFactory.getInstance(RSA_CIPHER_ALGORITHM).generatePrivate(privateKeySpec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] crypto(String cipherAlgorithm, int mode, Key key, byte[] data) {
        try {
            Cipher cipher = Cipher.getInstance(cipherAlgorithm);
            cipher.init(mode, key);
            return cipher.doFinal(data);
        } catch (NoSuchAlgorithmException | InvalidKeyException | NoSuchPaddingException | BadPaddingException
            | IllegalBlockSizeException e) {
            throw new IllegalStateException(e);
        }
    }

}
