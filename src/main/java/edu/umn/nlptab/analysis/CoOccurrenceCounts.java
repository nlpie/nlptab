/*
 * Copyright (c) 2015 Regents of the University of Minnesota.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umn.nlptab.analysis;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

/**
 *
 */
class CoOccurrenceCounts {
    public static final String ELASTIC_SEARCH_TYPE = "MatchCounts";

    private final Client client;

    private String index;

    private String analysisId;

    private long firstOnly = 0;

    private long secondOnly = 0;

    private long both = 0;

    private AnalysisConfig analysisConfig;

    @Inject
    CoOccurrenceCounts(Client client) {
        this.client = client;
    }

    void setAnalysisId(String analysisId) {
        this.analysisId = analysisId;
    }

    void setIndex(String index) {
        this.index = index;
    }

    void incrementFirstOnly() {
        firstOnly = Math.incrementExact(firstOnly);
    }

    void incrementSecondOnly() {
        secondOnly = Math.incrementExact(secondOnly);
    }

    void incrementBoth() {
        both = Math.incrementExact(both);
    }

    void add(CoOccurrenceCounts other) {
        firstOnly = Math.addExact(firstOnly, other.firstOnly);
        secondOnly = Math.addExact(secondOnly, other.secondOnly);
        both = Math.addExact(both, other.both);
    }

    IndexRequestBuilder buildRequest() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("analysisId", analysisId)
                .field("hitMissOnly", analysisConfig.isHitMiss());

        analysisConfig.append(xContentBuilder);

        if (analysisConfig.isHitMiss()) {
            xContentBuilder.field("hits", both)
                    .field("misses", firstOnly)
                    .field("accuracy", both / (double) (firstOnly + both))
            .endObject();
        } else {
            long twoTimesBoth = Math.multiplyExact(2, both);
            double fMeasure = twoTimesBoth / (double) Math.addExact(twoTimesBoth, Math.addExact(firstOnly, secondOnly));
            double recall = both / (double) Math.addExact(both, secondOnly);
            double precision = both / (double) Math.addExact(both, firstOnly);

            xContentBuilder.field("firstOnly", firstOnly)
                    .field("secondOnly", secondOnly)
                    .field("both", both)
                    .field("precision", precision)
                    .field("recall", recall)
                    .field("fMeasure", fMeasure)
                    .endObject();

        }

        return client.prepareIndex(index, ELASTIC_SEARCH_TYPE, analysisId)
                .setSource(xContentBuilder);
    }

    public void setAnalysisConfig(AnalysisConfig analysisConfig) {
        this.analysisConfig = analysisConfig;
    }
}
