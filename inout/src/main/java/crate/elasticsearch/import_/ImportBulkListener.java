package crate.elasticsearch.import_;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.util.concurrent.BaseFuture;

import crate.elasticsearch.import_.Importer.ImportCounts;

public class ImportBulkListener extends BaseFuture<ImportBulkListener> implements BulkProcessor.Listener {

    private AtomicLong bulksInProgress = new AtomicLong();
    private ImportCounts counts = new ImportCounts();

    public ImportBulkListener(String fileName) {
        counts.fileName = fileName;
    }

    @Override
    public ImportBulkListener get() throws InterruptedException,
            ExecutionException {
        if (bulksInProgress.get() == 0) {
            return this;
        }
        return super.get();
    }

    public void addFailure() {
        counts.failures++;
    }

    public ImportCounts importCounts() {
        return counts;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        bulksInProgress.incrementAndGet();
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request,
            BulkResponse response) {
        bulksInProgress.decrementAndGet();
        if (response.hasFailures()) {
            for (BulkItemResponse item : response.getItems()) {
                if (item.isFailed()) {
                    counts.failures++;
                } else {
                    counts.successes++;
                }
            }
        } else {
            counts.successes += response.getItems().length;
        }
        checkRelease();
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request,
            Throwable failure) {
        bulksInProgress.decrementAndGet();
        counts.failures += request.requests().size();
        failure.printStackTrace();
        checkRelease();
    }

    private void checkRelease() {
        if (bulksInProgress.get() == 0) {
            this.set(this);
        }
    }

    public void addInvalid() {
        counts.invalid++;
    }

}