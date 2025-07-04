import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
           System.out.println("Logs from your program will appear here!");

        int port = 6379;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected");

                // Start a new thread for each client
                new Thread(() -> handleClient(clientSocket)).start();
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (
            clientSocket; // This automatically closes the socket at the end
            OutputStream outputStream = clientSocket.getOutputStream()
        ) {
            byte[] input = new byte[1024];

            while (true) {
                int bytesRead = clientSocket.getInputStream().read(input);
                if (bytesRead == -1) {
                    break; // client closed connection
                }

                String str = new String(input, 0, bytesRead).trim();
                System.out.println("Received: " + str);

                outputStream.write("+PONG\r\n".getBytes());
            }
        } catch (IOException e) {
            System.out.println("Client disconnected or error: " + e.getMessage());
        }
    }
}