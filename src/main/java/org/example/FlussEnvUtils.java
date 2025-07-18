package org.example;

import py4j.CallbackClient;
import py4j.Gateway;
import py4j.GatewayServer;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The util class help to prepare Python env and run the python process.
 */
final class FlussEnvUtils {

    static final long CHECK_INTERVAL = 100;
    static final long TIMEOUT_MILLIS = 10000;

    /**
     * Creates a GatewayServer run in a daemon thread.
     *
     * @return The created GatewayServer
     */
    static GatewayServer startGatewayServer() throws ExecutionException, InterruptedException {
        CompletableFuture<GatewayServer> gatewayServerFuture = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try (FlussNetUtils.Port port = FlussNetUtils.getAvailablePort()) {
                int freePort = port.getPort();
                GatewayServer server = new GatewayServer.GatewayServerBuilder()
                        .gateway(new Gateway(new ConcurrentHashMap<String, Object>(), new CallbackClient(freePort)))
                        .javaPort(0)
                        .build();
                resetCallbackClientExecutorService(server);
                gatewayServerFuture.complete(server);
                server.start(true);
            } catch (Throwable e) {
                gatewayServerFuture.completeExceptionally(e);
            }
        });
        thread.setName("py4j-gateway");
        thread.setDaemon(true);
        thread.start();
        thread.join();
        return gatewayServerFuture.get();
    }

    /**
     * Reset a daemon thread to the callback client thread pool so that the callback server can be
     * terminated when gate way server is shutting down.
     */
    private static void resetCallbackClientExecutorService(GatewayServer gatewayServer)
            throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException,
                    InvocationTargetException {
        CallbackClient callbackClient = (CallbackClient) gatewayServer.getCallbackClient();
        Field executor = CallbackClient.class.getDeclaredField("executor");
        executor.setAccessible(true);
        ((ScheduledExecutorService) executor.get(callbackClient)).shutdown();
        executor.set(callbackClient, Executors.newScheduledThreadPool(1, Thread::new));
        Method setupCleaner = CallbackClient.class.getDeclaredMethod("setupCleaner");
        setupCleaner.setAccessible(true);
        setupCleaner.invoke(callbackClient);
    }

    /**
     * Reset the callback client of gatewayServer with the given callbackListeningAddress and
     * callbackListeningPort after the callback server started.
     */
    public static void resetCallbackClient(
            GatewayServer gatewayServer,
            String callbackServerListeningAddress,
            int callbackServerListeningPort)
            throws UnknownHostException, InvocationTargetException, NoSuchMethodException,
                    IllegalAccessException, NoSuchFieldException {
        gatewayServer.resetCallbackClient(
                InetAddress.getByName(callbackServerListeningAddress), callbackServerListeningPort);
        resetCallbackClientExecutorService(gatewayServer);
    }

    /**
     * Py4J both supports Java to Python RPC and Python to Java RPC. The GatewayServer object is the
     * entry point of Java to Python RPC. Since the Py4j Python client will only be launched only
     * once, the GatewayServer object needs to be reused.
     */
    private static GatewayServer gatewayServer = null;

    static void setGatewayServer(GatewayServer gatewayServer) {
        FlussEnvUtils.gatewayServer = gatewayServer;
    }
}
