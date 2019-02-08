package com.google.pubsub.flic.controllers.resource_controllers;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.flic.controllers.Client;
import com.google.pubsub.flic.controllers.ClientParams;
import com.google.pubsub.flic.controllers.ClientType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class LocalComputeResourceController extends ComputeResourceController {
    private static final Logger log = LoggerFactory.getLogger(LocalComputeResourceController.class);
    private final ClientParams params;
    private final int numWorkers;
    private final ScheduledExecutorService executor;
    private final Set<Process> clientProcesses = new HashSet<>();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public LocalComputeResourceController(
            ClientParams params, int numWorkers, ScheduledExecutorService executor) {
        super(executor);
        this.params = params;
        this.numWorkers = numWorkers;
        this.executor = executor;
    }

    @Override
    protected void startAction() {
    }

    private void runCpsProcess(ProcessBuilder builder) throws IOException {
        builder.environment().putAll(System.getenv());
        builder.inheritIO();
        Process process = builder.start();
        clientProcesses.add(process);
        executor.execute(() -> {
            try {
                process.waitFor();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            while (!shutdown.get()) {
                try {
                    runCpsProcess(builder);
                    break;
                } catch (IOException e) {
                    log.error("failed to start local process: " + e);
                }
            }
            clientProcesses.remove(process);
        });
    }

    private void runJavaProcess(ClientType.MessagingSide side, Integer port) throws IOException {
        String sideInfix = side == ClientType.MessagingSide.SUBSCRIBER ? "Subscriber" : "Publisher";
        ProcessBuilder builder = new ProcessBuilder(
                "java", "-Xmx5000M", "-cp", Client.RESOURCE_DIR + "/driver.jar",
                "com.google.pubsub.clients.gcloud.CPS" + sideInfix + "Task", "--port=" + port);
        runCpsProcess(builder);
    }

    private void runPythonProcess(ClientType.MessagingSide side, Integer port) throws IOException, InterruptedException {
        File tempDir = unzipToTemp();
        String sideInfix = side == ClientType.MessagingSide.SUBSCRIBER ? "subscriber" : "publisher";
        ProcessBuilder builder = new ProcessBuilder(
                "python3", "-m", "clients.cps_" + sideInfix + "_task", "--port=" + port);
        builder.directory(new File(tempDir, "python_src"));
        runCpsProcess(builder);
    }

    private void runNodeProcess(ClientType.MessagingSide side, Integer port) throws IOException, InterruptedException {
        File tempDir = unzipToTemp();
        String isPublisher = side == ClientType.MessagingSide.PUBLISHER ? "true" : "false";
        File nodeDir = new File(tempDir, "node_src");
        int installResult = new ProcessBuilder("npm", "install")
                .directory(nodeDir).start().waitFor();
        if (installResult != 0) {
            throw new IOException("Failed to install node project with error code: " + installResult);
        }
        ProcessBuilder builder = new ProcessBuilder(
                "node", "src/main.js", "--port=" + port, "--publisher=" + isPublisher);
        builder.directory(nodeDir);
        log.error("node command: " + String.join(", ", builder.command()));
        runCpsProcess(builder);
    }

    private void runGoProcess(ClientType.MessagingSide side, Integer port) throws IOException, InterruptedException {
        File tempDir = unzipToTemp();
        String isPublisher = side == ClientType.MessagingSide.PUBLISHER ? "true" : "false";
        ProcessBuilder builder = new ProcessBuilder(
                "go", "run", "main.go", "--port=" + port, "--publisher=" + isPublisher);
        builder.directory(new File(tempDir, "go_src/cmd"));
        runCpsProcess(builder);
    }

    private File unzipToTemp() throws IOException, InterruptedException {
        File dir = Files.createTempDirectory("cps_unzip").toFile();
        dir.deleteOnExit();
        ProcessBuilder builder = new ProcessBuilder(
                "unzip", Client.RESOURCE_DIR + "/cps.zip", "-d", dir.getCanonicalPath());
        int retval = builder.start().waitFor();
        if (retval != 0) {
            throw new IOException("Failed to unzip resource_controllers zip with error code: " + retval);
        }
        log.error("unzipped to: " + dir.getAbsolutePath());
        return dir;
    }

    private void runClientProcess(ClientType type, Integer port) throws Exception {
        switch (type.language) {
            case JAVA:
                runJavaProcess(type.side, port);
                return;
            case PYTHON:
                runPythonProcess(type.side, port);
                return;
            case NODE:
                runNodeProcess(type.side, port);
                return;
            case GO:
                runGoProcess(type.side, port);
                return;
        }
        log.error("LocalController does not yet support language: " + type.language);
        System.exit(1);
    }

    private int getPort() throws IOException {
        ServerSocket s = new ServerSocket(0);
        int port = s.getLocalPort();
        s.close();
        return port;
    }

    @Override
    public ListenableFuture<List<Client>> startClients() {
        SettableFuture<List<Client>> future = SettableFuture.create();
        executor.execute(() -> {
            List<Client> toReturn = new ArrayList<>();
            for (int i = 0; i < numWorkers; i++) {
                try {
                    Integer port = getPort();
                    runClientProcess(params.getClientType(), port);
                    toReturn.add(new Client("localhost", params, executor, port));
                } catch (Exception e) {
                    future.setException(e);
                    return;
                }
            }

            future.set(toReturn);
        });
        return future;
    }

    @Override
    protected void stopAction() {
        for (Process process : clientProcesses) {
            process.destroy();
        }
    }
}
