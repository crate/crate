package org.cratedb.blob.exceptions;

public class DigestMismatchException extends RuntimeException {

    public DigestMismatchException(String expected, String actual) {
        super("Expected " + expected + " got " + actual);
    }
}
