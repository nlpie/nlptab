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

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;

/**
 *
 */
public class SystemIndexingModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(SystemIndexingFactory.class)
                .toProvider(FactoryProvider.newFactory(SystemIndexingFactory.class, SystemIndexing.class));

        bind(SystemIndexingTaskFactory.class)
                .toProvider(FactoryProvider.newFactory(SystemIndexingTaskFactory.class, SystemIndexingTask.class));
    }
}
