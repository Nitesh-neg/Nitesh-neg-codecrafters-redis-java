import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
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

    public static void main(String[] args) {
        int port = 6379;
        System.out.println("Redis-like server started on port " + port);

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
                        long expiryTime = 0;

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

                    default:
                        outputStream.write("-ERR unknown command\r\n".getBytes());
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
