
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class RedisServer {

    private static final int PORT = 6379;

    private final miniRedis redis = new miniRedis();

    public void start() throws IOException {

        redis.startAutoSave();

        Runtime.getRuntime().addShutdownHook(
                new Thread(redis::close, "mini-redis-shutdown")
        );

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {

            System.out.println(
                    "Mini Redis server started on port " + PORT
            );

            while (true) {
                Socket client = serverSocket.accept();

                System.out.println("Client connected!");

                Thread.ofVirtual().start(() -> handleClient(client));
            }
        }
    }

    private void handleClient(Socket client) {
        try (
                client;
                BufferedReader input = new BufferedReader(
                        new InputStreamReader(client.getInputStream())
                );
                PrintWriter output = new PrintWriter(
                        client.getOutputStream(), true
                )
        ) {
            String command;

            while ((command = input.readLine()) != null) {

                if (command.equalsIgnoreCase("QUIT")) {
                    output.println("BYE");
                    break;
                }

                String response = redis.execute(command);
                output.println(response);
            }

        } catch (IOException e) {
            System.out.println("Client disconnected.");
        }
    }

    public static void main(String[] args) throws IOException {
        new RedisServer().start();
    }
}
