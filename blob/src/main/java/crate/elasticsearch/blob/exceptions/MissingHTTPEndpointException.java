package crate.elasticsearch.blob.exceptions;

public class MissingHTTPEndpointException extends Throwable {

    public MissingHTTPEndpointException(String message) {
        super(message);
    }
}
