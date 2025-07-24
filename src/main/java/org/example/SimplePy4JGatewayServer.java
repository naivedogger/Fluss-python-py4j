package org.example;

import py4j.GatewayServer;

/**
 * 简化的Py4J网关服务器
 * 直接启动网关服务器，不依赖复杂的环境变量和握手机制
 */
public class SimplePy4JGatewayServer {

    /**
     * 主方法，启动网关服务器
     * @param args 命令行参数，可以指定端口号
     */
    public static void main(String[] args) {
        try {
            // Entry Point for FlussGateway
            FlussGateway gateway = new FlussGateway();
            
            // start the Py4J gateway server
            int port = 25333;
            if (args.length > 0) {
                try {
                    port = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    System.out.println("无效的端口号，使用默认端口: " + port);
                }
            }
            
            GatewayServer server = new GatewayServer(gateway, port);
            server.start();
            
            System.out.println("Py4J网关服务器已启动");
            System.out.println("监听端口: " + server.getListeningPort());
            System.out.println("入口点: FlussGateway");
            System.out.println("服务器准备就绪，等待Python客户端连接...");
            
            // 保持服务器运行
            while (true) {
                Thread.sleep(1000);
            }
            
        } catch (Exception e) {
            System.err.println("启动网关服务器失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
