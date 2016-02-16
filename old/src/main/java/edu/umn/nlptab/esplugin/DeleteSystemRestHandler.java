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

package edu.umn.nlptab.esplugin;

import edu.umn.nlptab.core.InstanceIndexes;
import edu.umn.nlptab.systemindex.DeleteSystemTask;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.*;

/**
 *
 */
class DeleteSystemRestHandler implements RestHandler {

    private final Provider<DeleteSystemTask> deleteSystemTaskProvider;

    private final Client client;

    @Inject
    DeleteSystemRestHandler(Client client,
                            RestController restController,
                            Provider<DeleteSystemTask> deleteSystemTaskProvider) {
        this.client = client;

        restController.registerHandler(RestRequest.Method.POST, "_nlptab-deletesystem", this);

        this.deleteSystemTaskProvider = deleteSystemTaskProvider;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
        String instance = request.param("instance", "default");
        String id = request.param("id");

        InstanceIndexes instanceIndexes = new InstanceIndexes(instance);

        DeleteSystemTask deleteSystemTask = deleteSystemTaskProvider.get()
                .withSystemIndex(id)
                .withInstanceIndexes(instanceIndexes);

        deleteSystemTask.executeDeleteSystem();

        while (true) {
            SearchResponse searchResponse = client.prepareSearch(instanceIndexes.metadataIndex())
                    .setTypes("SystemIndex")
                    .setQuery(QueryBuilders.termQuery("index", id))
                    .get();

            if (searchResponse.getHits().totalHits() == 0) {
                break;
            }
        }

        XContentBuilder responseBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("status", "success")
                .endObject();
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, responseBuilder));
    }
}
