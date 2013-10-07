package org.cratedb;

import org.cratedb.blob.BlobContainer;
import org.cratedb.blob.DigestBlob;
import org.elasticsearch.common.bytes.BytesArray;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DigestBlobTests {


    @Test
    public void testDigestBlobResumeHeadAndAddContent() throws IOException {

        String digest = "417de3231e23dcd6d224ff60918024bc6c59aa58";
        UUID transferId = UUID.randomUUID();
        int currentPos = 2;

        BlobContainer container = new BlobContainer(new File("/tmp/crate-test"));
        File filePath = new File(container.getTmpDirectory(), String.format("%s.%s", digest, transferId.toString()));
        if (filePath.exists()) {
            filePath.delete();
        }

        DigestBlob digestBlob = DigestBlob.resumeTransfer(
            container, digest, transferId, currentPos);

        BytesArray contentHead = new BytesArray("A".getBytes());
        digestBlob.addToHead(contentHead);

        BytesArray contentTail = new BytesArray("CDEFGHIJKL".getBytes());
        digestBlob.addContent(contentTail, false);

        contentHead = new BytesArray("B".getBytes());
        digestBlob.addToHead(contentHead);

        contentTail = new BytesArray("MNO".getBytes());
        digestBlob.addContent(contentTail, true);

        byte[] buffer = new byte[15];
        FileInputStream stream = new FileInputStream(digestBlob.file());
        stream.read(buffer, 0, 15);
        assertEquals("ABCDEFGHIJKLMNO", new BytesArray(buffer).toUtf8().trim());
        File file = digestBlob.commit();
        assertTrue(file.exists());
        file.delete();
    }

    @Test
    public void testResumeDigestBlobAddHeadAfterContent() throws IOException {
        UUID transferId = UUID.randomUUID();
        BlobContainer container = new BlobContainer(new File("/tmp/crate-test"));
        DigestBlob digestBlob = DigestBlob.resumeTransfer(
            container, "417de3231e23dcd6d224ff60918024bc6c59aa58", transferId, 2);

        BytesArray contentTail = new BytesArray("CDEFGHIJKLMN".getBytes());
        digestBlob.addContent(contentTail, false);

        BytesArray contentHead = new BytesArray("AB".getBytes());
        digestBlob.addToHead(contentHead);

        contentTail = new BytesArray("O".getBytes());
        digestBlob.addContent(contentTail, true);

        byte[] buffer = new byte[15];
        FileInputStream stream = new FileInputStream(digestBlob.file());
        stream.read(buffer, 0, 15);
        assertEquals("ABCDEFGHIJKLMNO", new BytesArray(buffer).toUtf8().trim());
        File file = digestBlob.commit();
        assertTrue(file.exists());
        file.delete();
    }

}
