package org.example;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.Map;

import py4j.GatewayServer;
import py4j.Py4JPythonClient;

import java.io.File;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;

/**
 * Py4JGatewayServer 启动一个Py4J网关服务器
 * 允许Python客户端通过网络连接到Java进程并调用FlussClientBridge
 */
public class Py4JGatewayServer {

    /**
     * 主方法，启动网关服务器
     * @param args 命令行参数，可以指定端口号
     */
    public static void main(String[] args) 
            throws IOException, ExecutionException, InterruptedException, ClassNotFoundException {
        GatewayServer gatewayServer = FlussEnvUtils.startGatewayServer();
        FlussEnvUtils.setGatewayServer(gatewayServer);

        int boundPort = gatewayServer.getListeningPort();
        Py4JPythonClient callbackClient = gatewayServer.getCallbackClient();
        int callbackPort = callbackClient.getPort();
        if (boundPort == -1) {
            System.out.println("GatewayServer failed to bind; exiting");
            System.exit(1);
        }
        // Tells python side the port of our java rpc server
        String handshakeFilePath = System.getenv("_PYFLUSS_CONN_INFO_PATH");
        File handshakeFile = new File(handshakeFilePath);
        File tmpPath = Files.createTempFile(handshakeFile.getParentFile().toPath(), "connection", ".info").toFile();
        FileOutputStream fileOutputStream = new FileOutputStream(tmpPath);
        DataOutputStream stream = new DataOutputStream(new FileOutputStream(tmpPath));
        stream.writeInt(boundPort);
        stream.writeInt(callbackPort);
        stream.close();
        fileOutputStream.close();   
        
        if(!tmpPath.renameTo(handshakeFile)) {
            System.out.println("Unable to write connection information to handshake file: " + handshakeFilePath + ", now exit...");
            System.exit(1);
        }
        try {
            // This ensures that the server dies if its parent program dies.
            @SuppressWarnings("unchecked")
            Map<String, Object> entryPoint =
                    (Map<String, Object>) gatewayServer.getGateway().getEntryPoint();

            for (int i = 0; i < FlussEnvUtils.TIMEOUT_MILLIS / FlussEnvUtils.CHECK_INTERVAL; i++) {
                if (entryPoint.containsKey("Watchdog")) {
                    break;
                }
                Thread.sleep(FlussEnvUtils.CHECK_INTERVAL);
            }
            if (!entryPoint.containsKey("Watchdog")) {
                System.out.println("Unable to get the Python watchdog object, now exit.");
                System.exit(1);
            }
            Watchdog watchdog = (Watchdog) entryPoint.get("Watchdog");
            while (watchdog.ping()) {
                Thread.sleep(FlussEnvUtils.CHECK_INTERVAL);
            }
            gatewayServer.shutdown();
            System.exit(0);
        } finally {
            System.exit(1);
        }
    }
    
    public interface Watchdog {
        /**
         * Ping the watchdog to check if the server is still alive.
         * @return true if the server is alive, false otherwise.
         */
        boolean ping() throws InterruptedException;
    }
}
