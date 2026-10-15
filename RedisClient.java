
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class RedisClient {

    public static void main(String[] args) throws Exception {

        try (
            Socket socket = new Socket("127.0.0.1", 6379);
            BufferedReader input = new BufferedReader(
                new InputStreamReader(socket.getInputStream())
            );
            PrintWriter output = new PrintWriter(
                socket.getOutputStream(), true
            );
            BufferedReader keyboard = new BufferedReader(
                new InputStreamReader(System.in)
            )
        ) {

            System.out.println("Connected to Mini Redis!");

            while (true) {

                System.out.print("redis> ");

                String command = keyboard.readLine();

                if (command == null) {
                    break;
                }

                output.println(command);

                String response = input.readLine();

                if (response == null) {
                    System.out.println("Server disconnected.");
                    break;
                }

                System.out.println(response);

                if (command.equalsIgnoreCase("QUIT")) {
                    break;
                }
            }
        }
    }
}
