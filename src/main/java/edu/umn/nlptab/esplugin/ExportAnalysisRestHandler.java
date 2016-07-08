package edu.umn.nlptab.esplugin;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortParseElement;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

class ExportAnalysisRestHandler implements RestHandler {
    private final Client client;

    @Inject
    ExportAnalysisRestHandler(RestController restController, Client client) {
        restController.registerHandler(RestRequest.Method.GET, "_nlptab-analysis-export", this);

        this.client = client;
    }

    @Override
    public void handleRequest(RestRequest restRequest, RestChannel restChannel) throws Exception {
        String instance = restRequest.param("instance", "default");
        String id = restRequest.param("analysisId");

        BytesStreamOutput bytesStreamOutput = restChannel.bytesOutput();

        StringJoiner format = new StringJoiner(",", "", "\n");
        format.add("firstIsPresent");
        format.add("secondIsPresent");
        format.add("firstMatches");
        format.add("secondMatches");
        format.add("firstValues");
        format.add("secondValues");
        format.add("documentId");
        format.add("begin");
        format.add("end");
        bytesStreamOutput.writeBytes(format.toString().getBytes(StandardCharsets.UTF_8));

        SearchResponse searchResponse = client.prepareSearch(instance + "analysis")
                .setTypes("TruePositive", "FalsePositive", "FalseNegative")
                .setQuery(QueryBuilders.matchQuery("analysisId", id))
                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(5, TimeUnit.MINUTES))
                .setSize(100)
                .execute()
                .actionGet();

        while (true) {
            for (SearchHit hit : searchResponse.getHits()) {
                String type = hit.getType();
                Map<String, Object> source = hit.getSource();
                MatchUploadableFormatter matchUploadableFormatter = new MatchUploadableFormatter(type, source);
                bytesStreamOutput.writeBytes(matchUploadableFormatter.formatLine().getBytes(StandardCharsets.UTF_8));
            }

            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(5, TimeUnit.MINUTES))
                    .execute()
                    .actionGet();

            if (searchResponse.getHits().getHits().length == 0) {
                break;
            }
        }

        restChannel.sendResponse(new BytesRestResponse(RestStatus.ACCEPTED, "text/csv", bytesStreamOutput.bytes()));
    }

    private class MatchUploadableFormatter {
        private final Map<String, Object> matchUploadableSource;
        private final String type;

        MatchUploadableFormatter(String type, Map<String, Object> matchUploadableSource) {
            this.type = type;
            this.matchUploadableSource = matchUploadableSource;
        }

        String formatLine() {
            StringJoiner stringJoiner = new StringJoiner(",", "", "\n");
            stringJoiner.add(matchUploadableSource.get("firstIsPresent").toString());
            stringJoiner.add(matchUploadableSource.get("secondIsPresent").toString());
            stringJoiner.add(matchUploadableSource.get("firstMatches").toString());
            stringJoiner.add(matchUploadableSource.get("secondMatches").toString());
            stringJoiner.add(escape(matchUploadableSource.get("firstValues").toString()));
            stringJoiner.add(escape(matchUploadableSource.get("secondValues").toString()));
            stringJoiner.add(matchUploadableSource.get("documentId").toString());
            stringJoiner.add(matchUploadableSource.get("begin").toString());
            stringJoiner.add(matchUploadableSource.get("end").toString());

            return stringJoiner.toString();
        }

        private String escape(String string) {
            return String.format(Locale.ENGLISH, "\"%s\"", string.replaceAll("\"", "\"\"").replaceAll("'","''").replaceAll("\\\\", "\\\\\\\\"));
        }
    }
}
