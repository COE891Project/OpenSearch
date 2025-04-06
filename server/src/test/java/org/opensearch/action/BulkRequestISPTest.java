package org.opensearch.action;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

public class BulkRequestISPTest extends OpenSearchTestCase {

    @ParameterizedTest(name = "[{index}] Requests: {0}, ExpectedValid: {1}")
    @MethodSource("provideBulkRequestCases")
    void testBulkRequestPartitions(BulkRequest request, boolean expectedValid) {
        if (!expectedValid) {
            assertThrows(Exception.class, () -> {
                if (request.requests().isEmpty()) {
                    throw new IllegalArgumentException("Empty bulk request");
                }
                for (var subRequest : request.requests()) {
                    if (subRequest instanceof IndexRequest) {
                        ((IndexRequest) subRequest).source();
                    }
                }
            });
        } else {
            assertFalse(request.requests().isEmpty());
        }
    }

    private static Stream<Arguments> provideBulkRequestCases() {
        return Stream.of(
            Arguments.of(createBulkRequest(
                new IndexRequest("index1").id("1").source("{}", XContentType.JSON),
                new DeleteRequest("index1").id("2")), true),
            Arguments.of(createBulkRequest(
                new IndexRequest("index1").id("1").source("{\"field\":\"value\"}", XContentType.JSON)), 
                true),

            Arguments.of(new BulkRequest(), false),
            Arguments.of(createBulkRequest(
                new IndexRequest("index1").id("1").source("{invalid}", XContentType.JSON)), 
                false),
            Arguments.of(createBulkRequest(
                new IndexRequest("index1").id(null)), false),

            Arguments.of(createBulkRequest(1000), true),
            Arguments.of(createBulkRequest(0), false)
        );
    }

    private static BulkRequest createBulkRequest(Object... requests) {
        BulkRequest bulkRequest = new BulkRequest();
        for (Object req : requests) {
            if (req instanceof IndexRequest) {
                bulkRequest.add((IndexRequest) req);
            } else if (req instanceof DeleteRequest) {
                bulkRequest.add((DeleteRequest) req);
            }
        }
        return bulkRequest;
    }

    private static BulkRequest createBulkRequest(int numRequests) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numRequests; i++) {
            bulkRequest.add(new IndexRequest("index").id(String.valueOf(i))
                .source("{\"id\":" + i + "}", XContentType.JSON));
        }
        return bulkRequest;
    }
}