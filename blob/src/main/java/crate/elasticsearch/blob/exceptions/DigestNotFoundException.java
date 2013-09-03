package crate.elasticsearch.blob.exceptions;

import org.elasticsearch.ElasticSearchException;

public class DigestNotFoundException extends ElasticSearchException {

    public DigestNotFoundException(String digest) {
        super(digest);
    }
}
