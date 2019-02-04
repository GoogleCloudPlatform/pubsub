package com.google.pubsub.flic.controllers;

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
import java.util.concurrent.atomic.AtomicInteger;

public class LocalComputeResourceController extends ComputeResourceController {
    private static final AtomicInteger portTarget = new AtomicInteger(37298);

    private static final Logger log = LoggerFactory.getLogger(LocalComputeResourceController.class);
    private final String project;
    private final List<ClientParams> clients;
    private final ScheduledExecutorService executor;
    private final Set<Process> clientProcesses = new HashSet<>();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    LocalComputeResourceController(
            String project, List<ClientParams> clients, ScheduledExecutorService executor) {
        super(executor);
        this.project = project;
        this.clients = clients;
        this.executor = executor;
    }

    @Override
    protected void startAction() {}

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

    private void runJavaProcess(Client.MessagingSide side, Integer port) throws IOException {
        String sideInfix = side == Client.MessagingSide.SUBSCRIBER ? "Subscriber" : "Publisher";
        ProcessBuilder builder = new ProcessBuilder(
                "java", "-Xmx5000M", "-cp", Client.resourceDirectory+"/driver.jar",
                "com.google.pubsub.clients.gcloud.CPS" + sideInfix + "Task", "--port=" + port);
        runCpsProcess(builder);
    }

    private void runPythonProcess(Client.MessagingSide side, Integer port) throws IOException, InterruptedException {
        File tempDir = unzipToTemp();
        String sideInfix = side == Client.MessagingSide.SUBSCRIBER ? "subscriber" : "publisher";
        ProcessBuilder builder = new ProcessBuilder(
                "python3", "-m", "clients.cps_" + sideInfix + "_task", "--port=" + port);
        builder.directory(new File(tempDir, "python_src"));
        runCpsProcess(builder);
    }

    private void runNodeProcess(Client.MessagingSide side, Integer port) throws IOException, InterruptedException {
        File tempDir = unzipToTemp();
        String isPublisher = side == Client.MessagingSide.PUBLISHER ? "true" : "false";
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

    private void runGoProcess(Client.MessagingSide side, Integer port) throws IOException, InterruptedException {
        File tempDir = unzipToTemp();
        String isPublisher = side == Client.MessagingSide.PUBLISHER ? "true" : "false";
        ProcessBuilder builder = new ProcessBuilder(
                "go", "run", "main.go", "--port=" + port, "--publisher=" + isPublisher);
        builder.directory(new File(tempDir, "go_src/cmd"));
        runCpsProcess(builder);
    }

    private File unzipToTemp() throws IOException, InterruptedException {
        File dir = Files.createTempDirectory("cps_unzip").toFile();
        dir.deleteOnExit();
        ProcessBuilder builder = new ProcessBuilder(
                "unzip", Client.resourceDirectory + "/cps.zip", "-d", dir.getCanonicalPath());
        int retval = builder.start().waitFor();
        if (retval != 0) {
            throw new IOException("Failed to unzip resources zip with error code: " + retval);
        }
        log.error("unzipped to: " + dir.getAbsolutePath());
        return dir;
    }

    private void runClientProcess(Client.ClientType type, Integer port) throws Exception {
        if (type.language == Client.Language.JAVA) {
            runJavaProcess(type.side, port);
            return;
        }
        if (type.language == Client.Language.PYTHON) {
            runPythonProcess(type.side, port);
            return;
        }
        if (type.language == Client.Language.NODE) {
            runNodeProcess(type.side, port);
            return;
        }
        if (type.language == Client.Language.GO) {
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
    public List<Client> startClients() throws Exception {
        List<Client> toReturn = new ArrayList<>();
        for (ClientParams params : clients) {
            Integer port = getPort();
            runClientProcess(params.clientType, port);
            toReturn.add(new Client(params.clientType, "localhost", project, params.subscription, executor, port));
        }
        return toReturn;
    }

    @Override
    protected void stopAction() throws Exception {
        for (Process process : clientProcesses) {
            process.destroy();
        }
    }
}
