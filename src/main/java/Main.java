import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
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

            while (true) {
                List<String> command = parseRESP(clientSocket.getInputStream());
                      if (command.get(0).equalsIgnoreCase("PING")) {
                          outputStream.write("+PONG\r\n".getBytes());
                      }else if(command.get(0).equalsIgnoreCase("ECHO")){
                         String echoMsg = command.get(1);
                          String resp = "$" + echoMsg.length() + "\r\n" + echoMsg + "\r\n";
                          outputStream.write(resp.getBytes());
                     }else {
                          outputStream.write("-ERR unknown command\r\n".getBytes());
                      }
            }
        } catch (IOException e) {
            System.out.println("Client disconnected or error: " + e.getMessage());
        }
    }

          public static List<String> parseRESP(InputStream in) throws IOException {
          BufferedReader reader = new BufferedReader(new InputStreamReader(in));
          List<String> result = new ArrayList<>();

          String line = reader.readLine(); // *2
          if (line == null || !line.startsWith("*")) return result;

          int numArgs = Integer.parseInt(line.substring(1));
          for (int i = 0; i < numArgs; i++) {
              reader.readLine(); // Read $<length>, ignore for now
              String arg = reader.readLine(); // actual string
              result.add(arg);
          }

          return result;
      }

}