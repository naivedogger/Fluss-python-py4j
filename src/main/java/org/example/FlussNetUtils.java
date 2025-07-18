package org.example;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Utility for various network related tasks (such as finding free ports).
 */
public class FlussNetUtils {

    /**
     * Find a non-occupied port.
     *
     * @return A non-occupied port.
     */
    public static Port getAvailablePort() {
        for (int i = 0; i < 50; i++) {
            try (ServerSocket serverSocket = new ServerSocket(0)) {
                int port = serverSocket.getLocalPort();
                if (port != 0) {
                    return new Port(port);
                }
            } catch (IOException ignored) {
            }
        }
        throw new RuntimeException("Could not find a free permitted port on the machine.");
    }

    /**
     * Port wrapper class.
     */
    public static class Port implements AutoCloseable {
        private final int port;

        public Port(int port) {
            this.port = port;
        }

        public int getPort() {
            return port;
        }

        @Override
        public void close() throws Exception {
            // No cleanup needed for simple implementation
        }
    }
}
