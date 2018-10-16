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

import org.junit.Test;

import javax.crypto.Cipher;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

public class CryptoUtilsTest {

    static byte[] encryptRsaUsingPrivateKey(byte[] data, byte[] privateKeyBytes) {
        return CryptoUtils.crypto(CryptoUtils.RSA_CIPHER_ALGORITHM,
            Cipher.ENCRYPT_MODE,
            CryptoUtils.getPrivateKey(privateKeyBytes),
            data);
    }

    private static void writeKeysToFiles(final KeyPair keyPair,
                                 final Path publicKeyPath,
                                 final Path privateKeyPath) {
        try {
            Files.write(publicKeyPath, CryptoUtils.getPublicKeyBytes(keyPair.getPublic()));
            Files.write(privateKeyPath, CryptoUtils.getPrivateKeyBytes(keyPair.getPrivate()));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void testGenerateRsaKeysDoNotProduceNullKeys() {
        KeyPair keyPair = CryptoUtils.generateRsaKeyPair();

        assertThat(keyPair.getPrivate(), is(notNullValue()));
        assertThat(keyPair.getPublic(), is(notNullValue()));
    }

    @Test
    public void testRsaEncryptionDecryption() {
        KeyPair keyPair = CryptoUtils.generateRsaKeyPair();

        String data = "data";
        byte[] encrypt = encryptRsaUsingPrivateKey(data.getBytes(), CryptoUtils.getPrivateKeyBytes(keyPair.getPrivate()));
        assertThat(encrypt, is(notNullValue()));

        byte[] decrypt = CryptoUtils.decryptRsaUsingPublicKey(encrypt, CryptoUtils.getPublicKeyBytes(keyPair.getPublic()));
        assertThat(decrypt, is(notNullValue()));
        assertThat(new String(decrypt), is(data));
    }

    @Test
    public void testAesEncryptionDecryption() {
        String data = "data";
        byte[] encrypt = CryptoUtils.encryptAes(data.getBytes());
        assertThat(encrypt, is(notNullValue()));

        byte[] decrypt = CryptoUtils.decryptAes(encrypt);
        assertThat(decrypt, is(notNullValue()));
        assertThat(new String(decrypt), is(data));
    }
}
