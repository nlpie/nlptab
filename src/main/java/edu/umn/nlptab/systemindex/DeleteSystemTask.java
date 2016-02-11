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

package edu.umn.nlptab.systemindex;

import edu.umn.nlptab.core.InstanceIndexes;
import edu.umn.nlptab.NlpTabException;
import edu.umn.nlptab.core.ScrollDeleter;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 *
 */
public class DeleteSystemTask {

    private final Client client;

    private final Provider<ScrollDeleter> scrollDeleterProvider;

    private final DeleteOrphanedDocuments deleteOrphanedDocuments;

    private String systemIndex;

    private InstanceIndexes instanceIndexes;

    @Inject
    DeleteSystemTask(Client client,
                     Provider<ScrollDeleter> scrollDeleterProvider,
                     DeleteOrphanedDocuments deleteOrphanedDocuments) {
        this.client = client;
        this.scrollDeleterProvider = scrollDeleterProvider;
        this.deleteOrphanedDocuments = deleteOrphanedDocuments;
    }

    public DeleteSystemTask withSystemIndex(String systemIndex) {
        this.systemIndex = systemIndex;
        return this;
    }

    public DeleteSystemTask withInstanceIndexes(InstanceIndexes instanceIndexes) {
        this.instanceIndexes = instanceIndexes;
        return this;
    }

    public void executeDeleteSystem() throws NlpTabException {
        deleteSystemMetadata();
        deleteAnalysisResults();
        deleteMatchCounts();
        deleteDocumentsInSystem();
        deleteSystemIndex();
        deleteOrphanedDocuments.inSearchIndex(instanceIndexes.searchIndex()).run();
    }

    private void deleteDocumentsInSystem() {
        scrollDeleterProvider.get()
                .withIndexes(instanceIndexes.searchIndex())
                .withTypes("DocumentInSystem")
                .withQuery(QueryBuilders.termQuery("systemIndex", systemIndex))
                .executeDelete();
    }

    private void deleteMatchCounts() {
        scrollDeleterProvider.get()
                .withIndexes(instanceIndexes.analysisIndex())
                .withTypes("MatchCounts")
                .withQuery(QueryBuilders.termQuery("hypothesisUnitOfAnalysis.systemIndex", systemIndex))
                .executeDelete();

        scrollDeleterProvider.get()
                .withIndexes(instanceIndexes.analysisIndex())
                .withTypes("MatchCounts")
                .withQuery(QueryBuilders.termQuery("referenceUnitOfAnalysis.systemIndex", systemIndex))
                .executeDelete();
    }

    private void deleteSystemIndex() {
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        IndicesExistsResponse indicesExistsResponse = indicesAdminClient.prepareExists(systemIndex).get();
        if (indicesExistsResponse.isExists()) {
            indicesAdminClient.prepareDelete(systemIndex).get();
        }
    }

    private void deleteSystemMetadata() throws NlpTabException {
        SearchResponse searchResponse = client.prepareSearch(instanceIndexes.metadataIndex())
                .setTypes("SystemIndex")
                .setQuery(QueryBuilders.termQuery("index", systemIndex))
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.totalHits();
        if (totalHits == 0) {
            return;
        }

        if (totalHits != 1) {
            throw new NlpTabException("Specified index must only return one SystemIndex.");
        }

        SearchHit hit = hits.hits()[0];
        client.prepareDelete(hit.index(), hit.type(), hit.id()).get();
    }

    private void deleteAnalysisResults() {
        scrollDeleterProvider.get()
                .withIndexes(instanceIndexes.analysisIndex())
                .withTypes("TruePositive", "FalseNegative", "FalsePositive")
                .withQuery(QueryBuilders.termQuery("firstSystem", systemIndex))
                .executeDelete();

        scrollDeleterProvider.get()
                .withIndexes(instanceIndexes.analysisIndex())
                .withTypes("TruePositive", "FalseNegative", "FalsePositive")
                .withQuery(QueryBuilders.termQuery("secondSystem", systemIndex))
                .executeDelete();
    }
}
