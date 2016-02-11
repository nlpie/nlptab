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

package edu.umn.nlptab.core;

import edu.umn.nlptab.NlpTabException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;

class IndexInitializer {
    private static final ESLogger LOGGER = Loggers.getLogger(IndexInitializer.class);
    private final IndicesAdminClient client;
    private String index;
    private Settings settings;

    @Inject
    IndexInitializer(Client client) {
        this.client = client.admin().indices();
    }

    void setIndex(String index) {
        this.index = index;
    }

    void setSettings(Settings settings) {
        this.settings = settings;
    }

    void setSettingsFromClasspathResource(String resourcePath) throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            setSettings(Settings.settingsBuilder().loadFromStream(resourcePath, inputStream).build());
        }
    }

    void initializeIndex() throws NlpTabException {
        try {
            if (!client.prepareExists(index).execute().get().isExists()) {
                LOGGER.debug("Creating index: {} with settings: {}", index, settings.toString());
                if (!client.prepareCreate(index).setSettings(settings).execute().get().isAcknowledged()) {
                    LOGGER.error("Failed to create index: {} before updating settings", index);
                }
            } else {
                if (!client.prepareClose(index).execute().get().isAcknowledged()) {
                    LOGGER.error("Failed to close index: {} before updating settings", index);
                }

                LOGGER.debug("Adding settings to index: {}", index);
                if (!client.prepareUpdateSettings(index).setSettings(settings).execute().get().isAcknowledged()) {
                    LOGGER.error("Adding settings to index: {} failed", index);
                }

                if (!client.prepareOpen(index).execute().get().isAcknowledged()) {
                    LOGGER.error("Failed to open index: {} after updating settings", index);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new NlpTabException(e);
        }
    }
}
