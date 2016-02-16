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

import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

/**
 *
 */
public class SystemIndexingTask implements Runnable {

    public static final String TASK_ES_TYPE = "SystemIndexTask";

    private static final ESLogger LOGGER = Loggers.getLogger(SystemIndexingTask.class);

    private final Client client;

    private final SystemIndexingFactory systemIndexingFactory;

    private final SystemIndexingSettings systemIndexingSettings;

    private final SystemIndexingFiles systemIndexingFiles;

    private final String metadataIndex;

    private final String taskId;

    @Inject
    SystemIndexingTask(Client client,
                       SystemIndexingFactory systemIndexingFactory,
                       @Assisted SystemIndexingSettings systemIndexingSettings,
                       @Assisted SystemIndexingFiles systemIndexingFiles) {
        this.client = client;
        this.systemIndexingFactory = systemIndexingFactory;
        this.systemIndexingSettings = systemIndexingSettings;
        this.systemIndexingFiles = systemIndexingFiles;

        this.metadataIndex = systemIndexingSettings.getInstanceIndexes().metadataIndex();

        try {
            taskId = client.prepareIndex(metadataIndex, TASK_ES_TYPE)
                    .setId(systemIndexingSettings.getIndex())
                    .setSource(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("total", systemIndexingFiles.getDocumentFileCount())
                            .field("entityCount", 0)
                            .field("finished", false)
                            .field("failed", false)
                            .endObject())
                    .get()
                    .getId();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create system indexing task.", e);
        }
    }

    public String getTaskId() {
        return taskId;
    }

    @Override
    public void run() {
        int completedDocuments = 0;

        try {
            TypeSystemDescription typeSystemDescription = systemIndexingFiles.getTypeSystemDescription();
            SystemIndexing systemIndexing = systemIndexingFactory.create(systemIndexingSettings, typeSystemDescription);

            Iterator<Path> systemIndexingDocumentFiles = systemIndexingFiles.getSystemIndexingDocumentFiles();

            while (systemIndexingDocumentFiles.hasNext()) {
                Path documentPath = systemIndexingDocumentFiles.next();
                systemIndexing.indexDocument(documentPath);

                updateTask(++completedDocuments, false, false, null);
            }

            client.prepareUpdate(metadataIndex, "SystemIndex", systemIndexingSettings.getIndex())
                    .setDoc(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("ready", true)
                            .endObject())
                    .get();

            updateTask(completedDocuments, true, false, null);
        } catch (Throwable throwable) {
            updateTask(completedDocuments, true, true, throwable);
        }
    }

    private void updateTask(int completed, boolean finished, boolean failed, @Nullable Throwable throwable) {
        try {
            XContentBuilder doc = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("entityCount", completed)
                    .field("finished", finished)
                    .field("failed", failed);
            if (throwable != null) {
                doc.field("exception", throwable.toString())
                        .array("stackTrace", throwable.getStackTrace());
            }
            doc.endObject();

            client.prepareUpdate(metadataIndex, TASK_ES_TYPE, taskId)
                    .setDoc(doc)
                    .get();
        } catch (IOException e) {
            LOGGER.error("Error updating system indexing task document.", e);
        }
    }
}
