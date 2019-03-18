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

import org.elasticsearch.common.io.Streams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

final class EnterpriseLicense implements License {

    private static byte[] decrypt(byte[] encryptedContent) {
        return CryptoUtils.decryptRSAUsingPublicKey(encryptedContent, publicKey());

    }

    private static byte[] publicKey() {
        try (InputStream is = EnterpriseLicenseService.class.getResourceAsStream("/public.key")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            return out.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        } catch (NullPointerException e) {
            throw new IllegalStateException("The CrateDB distribution is missing its public key", e);
        }
    }

    private final int version;
    private final LicenseData licenseData;

    EnterpriseLicense(int version, byte[] encryptedContent) throws IOException {
        this.version = version;
        this.licenseData = LicenseConverter.fromJson(decrypt(encryptedContent), version);
    }

    @Override
    public Type type() {
        return Type.ENTERPRISE;
    }

    @Override
    public int version() {
        return version;
    }

    @Override
    public LicenseData licenseData() {
        return licenseData;
    }
}
