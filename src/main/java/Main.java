import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    static class ValueWithExpiry {
        String value;
        long expiryTimeMillis;

        ValueWithExpiry(String value, long expiryTimeMillis) {
            this.value = value;
            this.expiryTimeMillis = expiryTimeMillis;
        }
    }

    private static final Map<String, ValueWithExpiry> map = new HashMap<>();
    private static final Map<String, String> config = new HashMap<>();

    public static void main(String[] args) throws IOException {
        int port = 6379;
        System.out.println("Redis-like server started on port " + port);

         for (int i = 0; i < args.length; i++) {
          switch (args[i]) {
              case "--dir":
                  if (i + 1 < args.length) {
                      config.put("dir", args[i + 1]);
                      i++;
                  }
                  break;

              case "--dbfilename":
                  if (i + 1 < args.length) {
                      config.put("dbfilename", args[i + 1]);
                      i++;
                  }
                  break;

              default:
                  // Unknown argument, optionally skip or log
                  break;
          }
      }
            String dir_1 = config.get("dir");
            String dbfilename_1 = config.get("dbfilename");

            if (dir_1 != null && dbfilename_1 != null) {

                  Path filePath = Paths.get(dir_1).resolve(dbfilename_1);
                  byte[] fileBytes = Files.readAllBytes(filePath);


                   try (BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()))) {
                                String line;
                                List<String> lines = new ArrayList<>();

                                // Read whole file into lines (RDB is binary, but per your "line-based" approach)
                                while ((line = reader.readLine()) != null) {
                                    lines.add(line);
                                }

                                for (int i = 0; i < lines.size(); i++) {
                                    String currentLine = lines.get(i).trim();

                                    if (currentLine.startsWith("FC")) {
                                        System.out.println("Found FC marker at line " + i);

                                        // Skip next 2 lines (timestamp + value type)
                                        i += 2;

                                        if (i < lines.size()) {
                                            String keyHex = lines.get(i).trim();
                                            String key = hexToAscii(keyHex);
                                            map.put(key,new ValueWithExpiry("",1000));  // You can also read value similarly later
                                            System.out.println("Key Found: " + key);
                                        }
                                    }
                                }
                            }
                
            }

          

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected");

                new Thread(() -> handleClient(clientSocket)).start();
            }

        } catch (IOException e) {
            System.out.println("Server error: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (
            Socket socket = clientSocket;
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream()
        ) {
            while (true) {
                List<String> command = parseRESP(inputStream);
                if (command.isEmpty()) continue;

                System.out.println("Parsed RESP command: " + command);
                String cmd = command.get(0).toUpperCase();

                switch (cmd) {
                    case "PING":
                        outputStream.write("+PONG\r\n".getBytes());
                        break;

                    case "ECHO":
                        String echoMsg = command.get(1);
                        String resp = "$" + echoMsg.length() + "\r\n" + echoMsg + "\r\n";
                        outputStream.write(resp.getBytes());
                        break;

                    case "SET":
                        String key = command.get(1);
                        String value = command.get(2);
                        long expiryTime = 1000;

                        if (command.size() >= 5 && command.get(3).equalsIgnoreCase("PX")) {
                            try {
                                long pxMillis = Long.parseLong(command.get(4));
                                expiryTime = System.currentTimeMillis() + pxMillis;
                            } catch (NumberFormatException e) {
                                outputStream.write("-ERR invalid PX value\r\n".getBytes());
                                continue;
                            }
                        }

                        map.put(key, new ValueWithExpiry(value, expiryTime));
                        outputStream.write("+OK\r\n".getBytes());
                        break;

                    case "GET":
                        String getKey = command.get(1);
                        ValueWithExpiry stored = map.get(getKey);

                        if (stored == null || (stored.expiryTimeMillis != 0 && System.currentTimeMillis() > stored.expiryTimeMillis)) {
                            map.remove(getKey);
                            outputStream.write("$-1\r\n".getBytes());
                        } else {
                            String getResp = "$" + stored.value.length() + "\r\n" + stored.value + "\r\n";
                            outputStream.write(getResp.getBytes());
                        }
                        break;

                    case "CONFIG":
                       if (command.size() >= 3 && command.get(1).equalsIgnoreCase("GET")) {
                              String key_1 = command.get(2);
                              String value_1 = config.get(key_1);
                              if (value_1 != null) {
                                  String respConfig = "*2\r\n" +
                                          "$" + key_1.length() + "\r\n" + key_1 + "\r\n" +
                                          "$" + value_1.length() + "\r\n" + value_1 + "\r\n";
                                  outputStream.write(respConfig.getBytes());
                              } else {
                                  outputStream.write("*0\r\n".getBytes()); // RESP empty array
                              }
                          } else {
                              outputStream.write("-ERR wrong CONFIG usage\r\n".getBytes());
                          }
                          break;

                   case "KEYS":
                           if (command.get(1).equals("*")) {
                              StringBuilder respKeys = new StringBuilder();
                              respKeys.append("*").append(map.size()).append("\r\n");
                              for (String key_2 : map.keySet()) {
                                  respKeys.append("$").append(key_2.length()).append("\r\n")
                                          .append(key_2).append("\r\n");
                              }
                              outputStream.write(respKeys.toString().getBytes());
                          }
                          break;


                    default:
                        outputStream.write("- unknown command\r\n".getBytes());
                }
            }
        } catch (IOException e) {
            System.out.println("Client disconnected or error: " + e.getMessage());
        }
    }

    public static List<String> parseRESP(InputStream in) throws IOException {
        List<String> result = new ArrayList<>();
        DataInputStream reader = new DataInputStream(in);

        int b = reader.read();
        if (b == -1) {
            return result;
        }

        if ((char) b != '*') {
            throw new IOException("Expected RESP array (starts with '*')");
        }

        int numArgs = Integer.parseInt(readLine(reader));
        for (int i = 0; i < numArgs; i++) {
            char prefix = (char) reader.read();
            if (prefix != '$') {
                throw new IOException("Expected bulk string (starts with '$')");
            }

            int length = Integer.parseInt(readLine(reader));
            byte[] buf = new byte[length];
            reader.readFully(buf);
            result.add(new String(buf));

            // Read and discard trailing \r\n
            readLine(reader);
        }


        return result;
    }

    private static String readLine(DataInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            char c = (char) in.readByte();
            if (c == '\r') {
                char next = (char) in.readByte();
                if (next == '\n') break;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public static String hexToAscii(String hex) {
        String[] parts = hex.split("\\s+");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            int val = Integer.parseInt(part, 16);
            sb.append((char) val);
        }
        return sb.toString();
    }
}
