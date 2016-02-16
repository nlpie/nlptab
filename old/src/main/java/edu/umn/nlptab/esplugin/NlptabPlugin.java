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

import edu.umn.nlptab.casprocessing.CasProcessingModule;
import edu.umn.nlptab.systemindex.SystemIndexingModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptModule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Elasticsearch plugin for the NLP-TAB app.
 *
 * @author Ben Knoll
 * @since 1.0
 */
public class NlptabPlugin extends Plugin {
    @Override
    public String name() {
        return "nlptab";
    }

    @Override
    public String description() {
        return "NLP-TAB elasticsearch plugin";
    }

    @Override
    public Collection<Module> nodeModules() {
        return Arrays.asList(new NlptabModule(), new CasProcessingModule(), new SystemIndexingModule());
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        return Collections.singleton(NlptabService.class);
    }

    /**
     * Initializes the lucene scripts used by NLP-TAB. Called by Elasticsearch using reflection.
     *
     * @param scriptModule injected script module.
     * @see Plugin
     */
    @SuppressWarnings("unused")
    public void onModule(ScriptModule scriptModule) {
        scriptModule.registerScript("annotationDistance", AnnotationDistanceScriptFactory.class);
    }
}
