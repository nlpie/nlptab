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

import edu.umn.nlptab.systemindex.SystemIndexSetup;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.*;

import java.util.Map;

/**
 * Responsible for the first part of system indexing, setting up the metadata and preparing for indexing.
 * <p>Handles a json object with the following format:
 * <pre>
 *     {
 *         "systemName": "system-name",
 *         "systemDescription": "A description of the system",
 *         "instance": "instance"
 *     }
 * </pre>
 * and returns a json object containing the index of the system created:
 * <pre>
 *     {
 *         "index": "created-index"
 *     }
 * </pre>
 * </p>
 *
 * @author Ben Knoll
 * @since 1.0
 */
class SystemIndexingMetaRestHandler implements RestHandler {
    private final Provider<SystemIndexSetup> systemIndexSetupProvider;

    @Inject
    SystemIndexingMetaRestHandler(RestController restController, Provider<SystemIndexSetup> systemIndexSetupProvider) {
        this.systemIndexSetupProvider = systemIndexSetupProvider;
        restController.registerHandler(RestRequest.Method.POST, "_nlptab-systemindexmeta", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
        SystemIndexSetup systemIndexSetup = systemIndexSetupProvider.get();

        BytesReference content = request.content();
        Map<String, Object> map = JsonXContent.jsonXContent.createParser(content).map();
        systemIndexSetup.initFromJsonMap(map);

        systemIndexSetup.setupElasticSearch();
        systemIndexSetup.uploadMetadata();

        String primaryIndex = systemIndexSetup.getPrimaryIndex();

        channel.sendResponse(new BytesRestResponse(RestStatus.OK, XContentFactory.jsonBuilder()
                .startObject()
                .field("index", primaryIndex)
                .endObject()));
    }
}
