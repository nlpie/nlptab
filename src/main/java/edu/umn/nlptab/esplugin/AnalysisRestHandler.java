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

import edu.umn.nlptab.analysis.AnalysisConfig;
import edu.umn.nlptab.analysis.AnalysisConfigurationException;
import edu.umn.nlptab.analysis.AnalysisRunner;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.*;

import java.util.Map;

/**
 * Rest handler which biomedicus analysis.
 *
 * @author Ben Knoll
 * @since 1.0
 */
class AnalysisRestHandler implements RestHandler {

    private final Provider<AnalysisRunner> analysisRunnerProvider;

    private final Provider<AnalysisConfig> analysisConfigProvider;

    private final NlptabService nlptabService;

    @Inject
    AnalysisRestHandler(RestController restController,
                        Provider<AnalysisRunner> analysisRunnerProvider,
                        Provider<AnalysisConfig> analysisConfigProvider,
                        NlptabService nlptabService) {
        this.analysisConfigProvider = analysisConfigProvider;
        restController.registerHandler(RestRequest.Method.POST, "/_nlptab-analysis", this);
        this.analysisRunnerProvider = analysisRunnerProvider;
        this.nlptabService = nlptabService;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
        AnalysisRunner analysisRunner = analysisRunnerProvider.get();

        BytesReference content = request.content();

        XContentParser parser = JsonXContent.jsonXContent.createParser(content);

        Map<String, Object> map = parser.map();

        try {
            AnalysisConfig analysisConfig = analysisConfigProvider.get();
            analysisConfig.initFromMap(map);
            analysisRunner.setAnalysisConfig(analysisConfig);
        } catch (AnalysisConfigurationException e) {
            String message = e.getLocalizedMessage();
            channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, message == null ? "Error processing config" : message));
            return;
        }

        String id = Strings.base64UUID();
        analysisRunner.setId(id);

        nlptabService.submit(analysisRunner::setupIndexAndPerformAnalysis);

        XContentBuilder responseBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", id)
                .endObject();
        channel.sendResponse(new BytesRestResponse(RestStatus.ACCEPTED, responseBuilder));
    }
}
