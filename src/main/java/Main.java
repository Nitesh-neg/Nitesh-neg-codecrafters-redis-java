import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;


public class Main {

      static class ValueWithExpiry {
        String value;
        long expiryTimeMillis; 

        ValueWithExpiry(String value, long expiryTimeMillis) {
            this.value = value;
            this.expiryTimeMillis = expiryTimeMillis;
        }
    }

  private static final Map<String, ValueWithExpiry> map = new HashMap<>();// for getting expiry time too.

  public static void main(String[] args){
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
            OutputStream outputStream = clientSocket.getOutputStream();
        ) {

            while (true) {
                List<String> command = parseRESP(clientSocket.getInputStream());

                      if (command.get(0).equalsIgnoreCase("PING")) {
                          outputStream.write("+PONG\r\n".getBytes());

                      }else if(command.get(0).equalsIgnoreCase("ECHO")){

                          String echoMsg = command.get(1);
                          String resp = "$" + echoMsg.length() + "\r\n" + echoMsg + "\r\n";
                          outputStream.write(resp.getBytes());

                     }else if(command.get(0).equalsIgnoreCase("SET")){

                      //    map.put(command.get(1), command.get(2)); // For SET
                            String key = command.get(1);
                            String value = command.get(2);

                            long expiryTime = 0;

                            // Check for PX argument (case-insensitive)
                            if (command.size() >= 5 && command.get(3).equalsIgnoreCase("PX")) {
                                try {
                                    long pxMillis = Long.parseLong(command.get(4));
                                    expiryTime = System.currentTimeMillis() + pxMillis;
                                } catch (NumberFormatException e) {
                                    outputStream.write("-ERR invalid PX value\r\n".getBytes());
                                    break;
                                }
                            }

                            map.put(key, new ValueWithExpiry(value, expiryTime));
                            outputStream.write("+OK\r\n".getBytes());
                            break;

                     }else if(command.get(0).equalsIgnoreCase("GET")){
 
                              ValueWithExpiry stored = map.get(command.get(1));
                              if (stored == null) {
                                  outputStream.write("$-1\r\n".getBytes());
                              } else if (stored.expiryTimeMillis != 0 && System.currentTimeMillis() > stored.expiryTimeMillis) {
                                  map.remove(command.get(1)); // Clean up expired key
                                  outputStream.write("$-1\r\n".getBytes());
                              } else {
                                  String resp = "$" + stored.value.length() + "\r\n" + stored.value + "\r\n";
                                  outputStream.write(resp.getBytes());
                              }
                              break;
                                            
                     }else{
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

          int numArgs = Integer.parseInt(line.substring(1));// total arguments

          for (int i = 0; i < numArgs; i++) {

              reader.readLine(); // Read $<length>, ignore for now
              String arg = reader.readLine(); // actual string
              result.add(arg);

          }

          return result;
      }

}