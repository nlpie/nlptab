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

import edu.umn.nlptab.systemindex.DeleteOrphanedDocuments;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.*;

/**
 *
 */
class DeleteOrphanedDocumentsRestHandler implements RestHandler {
    private final Provider<DeleteOrphanedDocuments> deleteOrphanedDocumentsProvider;

    @Inject
    DeleteOrphanedDocumentsRestHandler(RestController restController,
                                       Provider<DeleteOrphanedDocuments> deleteOrphanedDocumentsProvider) {
        restController.registerHandler(RestRequest.Method.GET, "_nlptab-delete-orphaned-documents", this);
        this.deleteOrphanedDocumentsProvider = deleteOrphanedDocumentsProvider;
    }


    @Override
    public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
        String searchIndex = request.param("searchIndex");

        deleteOrphanedDocumentsProvider.get().inSearchIndex(searchIndex).run();

        XContentBuilder status = XContentFactory.jsonBuilder().startObject().field("success", true).endObject();
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, status));
    }
}
