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

import com.google.common.base.Preconditions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * NLP-TAB node level service.
 *
 * @author Ben Knoll
 * @since 1.0
 */
class NlptabService extends AbstractLifecycleComponent<NlptabService> {
    private final Environment environment;
    /**
     * Executor service for running tasks. Is null until service start and is shutdown along with the nlp-tab service.
     */
    @Nullable
    private ExecutorService executorService;

    /**
     * Default constructor.
     */
    @Inject
    NlptabService(Environment environment) {
        super(Settings.EMPTY);
        this.environment = environment;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        executorService = Executors.newFixedThreadPool(2, EsExecutors.daemonThreadFactory("nlptab"));

        System.setProperty("uima.datapath", environment.tmpFile().toString());
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        executorService = null;
    }

    /**
     * @see ExecutorService#invokeAny(Collection, long, TimeUnit)
     */
    <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        Preconditions.checkNotNull(executorService);
        return executorService.invokeAny(tasks, timeout, unit);
    }

    /**
     * @see ExecutorService#invokeAny(Collection)
     */
    <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        Preconditions.checkNotNull(executorService);
        return executorService.invokeAny(tasks);
    }

    /**
     * @see ExecutorService#invokeAll(Collection, long, TimeUnit)
     */
    <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        Preconditions.checkNotNull(executorService);
        return executorService.invokeAll(tasks, timeout, unit);
    }

    /**
     * @see ExecutorService#invokeAll(Collection)
     */
    <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        Preconditions.checkNotNull(executorService);
        return executorService.invokeAll(tasks);
    }

    /**
     * @see ExecutorService#submit(Runnable)
     */
    Future<?> submit(Runnable task) {
        Preconditions.checkNotNull(executorService);
        return executorService.submit(task);
    }

    /**
     * @see ExecutorService#submit(Runnable, Object)
     */
    <T> Future<T> submit(Runnable task, T result) {
        Preconditions.checkNotNull(executorService);
        return executorService.submit(task, result);
    }

    /**
     * @see ExecutorService#submit(Callable)
     */
    <T> Future<T> submit(Callable<T> task) {
        Preconditions.checkNotNull(executorService);
        return executorService.submit(task);
    }
}
