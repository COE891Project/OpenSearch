package org.opensearch.action;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

public class SearchRequestISPTest extends OpenSearchTestCase {

    @ParameterizedTest(name = "[{index}] From: {0}, Size: {1}, SortField: {2}, ExpectedValid: {3}")
    @MethodSource("provideSearchRequestCases")
    void testSearchRequestPartitions(int from, int size, String sortField, boolean expectedValid) {
        SearchRequest request = new SearchRequest("test_index");
        SearchSourceBuilder source = new SearchSourceBuilder();
        
        try {
            source.from(from).size(size);
            if (sortField != null) {
                source.sort(sortField);
            }
            request.source(source);
            
            if (!expectedValid) {
                fail("Expected validation failure but passed");
            }
            
            assertEquals(from, source.from());
            assertEquals(size, source.size());
        } catch (IllegalArgumentException e) {
            if (expectedValid) {
                fail("Unexpected validation failure: " + e.getMessage());
            }
        }
    }

    private static Stream<Arguments> provideSearchRequestCases() {
        return Stream.of(
            Arguments.of(0, 10, "timestamp", true), // Normal case
            Arguments.of(0, 10000, null, true), // Max size boundary
            Arguments.of(9999, 1, "_score", true), // High offset
            
            Arguments.of(-1, 10, null, false), // Negative from
            Arguments.of(0, -1, null, false), // Negative size
            Arguments.of(0, 10001, null, false), // Exceeds max size
            Arguments.of(0, 10, "nonexistent_field", true), // Invalid sort (behavior varies)
            
            Arguments.of(Integer.MAX_VALUE, 1, null, false), // Overflow risk
            Arguments.of(0, 0, null, true) // Zero results
        );
    }
}