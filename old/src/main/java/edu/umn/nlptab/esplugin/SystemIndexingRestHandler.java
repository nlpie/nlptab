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

import edu.umn.nlptab.systemindex.SystemIndexingSettings;
import edu.umn.nlptab.systemindex.SystemIndexingTask;
import edu.umn.nlptab.systemindex.SystemIndexingTaskFactory;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.*;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Creates a route: "_nlptab-systemindex" which accepts a zip file containing XMI or XCAS XML files. The zip file must
 * also contain a "TypeSystem.xml" file containing the type system of the XMI/XCAS files. The route takes 3 parameters:
 * <ol>
 * <li>instance - the elasticsearch index to upload to</li>
 * <li>index - the system index to upload to</li>
 * <li>useXCas - whether the files are serialized in the XCas format, XMI is the default.</li>
 * </ol>
 *
 * @author Ben Knoll
 * @since 1.0
 */
class SystemIndexingRestHandler implements RestHandler {
    /**
     * Factory for system indexing tasks.
     */
    private final SystemIndexingTaskFactory systemIndexingTaskFactory;

    /**
     * The nlptab service used to run tasks.
     */
    private final NlptabService nlptabService;

    /**
     * Elasticsearch application environment.
     */
    private final Environment environment;

    /**
     * Injected constructor.
     *
     * @param restController            to register this rest handler.
     * @param systemIndexingTaskFactory provider for a indexing task.
     * @param nlptabService             nlptab service for running tasks.
     * @param environment               environment for getting temp directory.
     */
    @Inject
    SystemIndexingRestHandler(RestController restController,
                              SystemIndexingTaskFactory systemIndexingTaskFactory,
                              NlptabService nlptabService,
                              Environment environment) {
        restController.registerHandler(RestRequest.Method.POST, "_nlptab-systemindex", this);

        this.systemIndexingTaskFactory = systemIndexingTaskFactory;
        this.nlptabService = nlptabService;
        this.environment = environment;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
        String instance = Objects.requireNonNull(request.param("instance"));
        String index = Objects.requireNonNull(request.param("index"));
        boolean useXCas = request.hasParam("useXCas") && Boolean.parseBoolean(request.param("useXCas"));

        BytesReference content = request.content();

        Path tmpFolder = environment.tmpFile();
        Path zipPath = tmpFolder.resolve(Strings.base64UUID());
        try (OutputStream out = Files.newOutputStream(zipPath, StandardOpenOption.CREATE_NEW)) {
            content.writeTo(out);
        }

        SystemIndexingSettings systemIndexingSettings = new SystemIndexingSettings(instance, index, useXCas);

        ZipSystemIndexingFiles zipSystemIndexingFiles = new ZipSystemIndexingFiles(zipPath, useXCas);

        SystemIndexingTask systemIndexingTask = systemIndexingTaskFactory.create(systemIndexingSettings,
                zipSystemIndexingFiles);

        String taskId = systemIndexingTask.getTaskId();

        nlptabService.submit(systemIndexingTask);

        XContentBuilder jsonResponse = XContentFactory.jsonBuilder()
                .startObject()
                .field("index", taskId)
                .endObject();
        channel.sendResponse(new BytesRestResponse(RestStatus.ACCEPTED, jsonResponse));
    }
}
