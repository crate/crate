/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.license;

import org.junit.Test;

import javax.crypto.Cipher;
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

    @Test
    public void testGenerateRsaKeysDoNotProduceNullKeys() {
        KeyPair keyPair = CryptoUtils.generateRSAKeyPair();

        assertThat(keyPair.getPrivate(), is(notNullValue()));
        assertThat(keyPair.getPublic(), is(notNullValue()));
    }

    @Test
    public void testRsaEncryptionDecryption() {
        KeyPair keyPair = CryptoUtils.generateRSAKeyPair();

        String data = "data";
        byte[] encrypt = encryptRsaUsingPrivateKey(data.getBytes(), CryptoUtils.getPrivateKeyBytes(keyPair.getPrivate()));
        assertThat(encrypt, is(notNullValue()));

        byte[] decrypt = CryptoUtils.decryptRSAUsingPublicKey(encrypt, CryptoUtils.getPublicKeyBytes(keyPair.getPublic()));
        assertThat(decrypt, is(notNullValue()));
        assertThat(new String(decrypt), is(data));
    }

    @Test
    public void testAesEncryptionDecryption() {
        String data = "data";
        byte[] encrypt = CryptoUtils.encryptAES(data.getBytes());
        assertThat(encrypt, is(notNullValue()));

        byte[] decrypt = CryptoUtils.decryptAES(encrypt);
        assertThat(decrypt, is(notNullValue()));
        assertThat(new String(decrypt), is(data));
    }
}
