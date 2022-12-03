/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.tools;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to create a simple HTTP server listening on a specific port and handling requests by a
 * specified handler.
 *
 * @author sykorjan
 */
public class SimpleHttpServer {

    private HttpServer httpServer;

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleHttpServer.class);

    /**
     * Instantiate a new HTTP server. Use port number 0 (zero) to let the system select a new port number.
     *
     * @param port    number of port
     * @throws IOException if could not create the server
     */
    public SimpleHttpServer(int port) throws IOException {
        // Maximum number of incoming TCP connections is set to system default value
        int backlog = 0;
        // Create HTTP server
        httpServer = HttpServer.create(new InetSocketAddress(port), backlog);
        // Create an executor
        httpServer.setExecutor(Executors.newFixedThreadPool(4 * Runtime.getRuntime().availableProcessors()));
    }

    public void createContext(String context, HttpHandler handler) {
        // Create HTTP context with a given handler
        httpServer.createContext(context, handler);
    }

    /**
     * Start server
     */
    public void start() {
        httpServer.start();
        LOGGER.info("Server started and is listening on port {}", httpServer.getAddress().getPort());
    }

    /**
     * Stop server
     */
    public void stop() {
        // stop immediately
        ((ExecutorService) httpServer.getExecutor()).shutdown();
        httpServer.stop(0);
        LOGGER.info("Server stopped");
    }

    public InetSocketAddress getAddress() {
        return httpServer.getAddress();
    }

}
