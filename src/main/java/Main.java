import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

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
                  break;
          }
      }
     
      if (config.get("dir") != null && config.get("dbfilename") != null) {
            final Path path = Paths.get(config.get("dir") + '/' + config.get("dbfilename"));
            final byte[] bytes;
            try {
                bytes = Files.readAllBytes(path);
                
                int databaseSectionOffset = -1;
                for (int i = 0; i < bytes.length; i++) {
                    if (bytes[i] == (byte) 0xfe) {
                        databaseSectionOffset = i;
                        break;
                    }
                }

                for (int i = databaseSectionOffset + 4; i < bytes.length; i++) {
                    if (bytes[i] == (byte) 0x00 && i + 1 < bytes.length) {//0x00 is the marker before the key string.
                        final int keyStrLen = bytes[i + 1] & 0xFF;  // why using 0xFF --> because byte value in rdb file is from 0 to 255 , but in byte java , it is considered from -128 to 127
                        if (keyStrLen <= 0) continue;
                        final byte[] keyBytes = new byte[keyStrLen];
                        for (int j = i + 2; j < i + 2 + keyStrLen; j++) {
                            keyBytes[j - (i + 2)] = bytes[j];
                        }

                       i += 2 + keyStrLen;  // Move past key length prefix and key bytes
                        if (i >= bytes.length) break;

                        final int valueStrLen = bytes[i] & 0xFF;
                        if (valueStrLen <= 0) continue;

                        if (i + 1 + valueStrLen >= bytes.length) break;  // Safety check

                        // Read value bytes BEFORE moving i:
                        final byte[] valueBytes = new byte[valueStrLen];
                        for (int j = 0; j < valueStrLen; j++) {
                            valueBytes[j] = bytes[i + 1 + j];
                        }

                        // Now advance i after value bytes:
                        i += 1 + valueStrLen;

                        // Now read expiry (if present):
                        if (i < bytes.length && bytes[i] == (byte) 0xFC && i + 8 < bytes.length) {
                            byte[] timestampBytes = Arrays.copyOfRange(bytes, i + 1, i + 9);
                            ByteBuffer buffer = ByteBuffer.wrap(timestampBytes).order(ByteOrder.LITTLE_ENDIAN);
                            long expiryTime = buffer.getLong();
                            i += 8;  // Move past expiry bytes
                            map.put(new String(keyBytes), new ValueWithExpiry(new String(valueBytes), expiryTime));
                        } else {
                            map.put(new String(keyBytes), new ValueWithExpiry(new String(valueBytes), Long.MAX_VALUE));
                        }
                    }

                }

            } catch (IOException e) {
                System.out.println("RDB file not found or error reading it: " + e);
                // Continue with empty DB
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
                        long expiryTime = Long.MAX_VALUE;

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

}