import java.io.*;
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

        // Parse command line arguments
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

        // Load RDB file if configured
        if (config.get("dir") != null && config.get("dbfilename") != null) {
            Path rdbPath = Paths.get(config.get("dir"), config.get("dbfilename"));
            if (Files.exists(rdbPath)) {
                loadRdbFile(rdbPath);
            }
        }

        // Start server
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server listening on port " + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        }
    }

    private static void loadRdbFile(Path rdbPath) {
        try (InputStream in = Files.newInputStream(rdbPath)) {
            byte[] magic = new byte[5];
            in.read(magic);
            if (!new String(magic).equals("REDIS")) {
                throw new IOException("Invalid RDB file format");
            }

            // Skip version (9 bytes total for "REDIS" + version)
            in.skip(4);

            while (true) {
                int opcode = in.read();
                if (opcode == -1) break;

                switch (opcode) {
                    case 0xFC: // Expiry time in milliseconds
                        long expiryMillis = readRdbLength(in) * 1000L;
                        int keyLen = (int) readRdbLength(in);
                        String key = readRdbString(in, keyLen);
                        int valLen = (int) readRdbLength(in);
                        String value = readRdbString(in, valLen);
                        map.put(key, new ValueWithExpiry(value, System.currentTimeMillis() + expiryMillis));
                        break;
                    case 0xFD: // Expiry time in seconds
                        long expirySeconds = Integer.reverseBytes(in.read()) & 0xFFFFFFFFL;
                        keyLen = (int) readRdbLength(in);
                        key = readRdbString(in, keyLen);
                        valLen = (int) readRdbLength(in);
                        value = readRdbString(in, valLen);
                        map.put(key, new ValueWithExpiry(value, System.currentTimeMillis() + (expirySeconds * 1000)));
                        break;
                    case 0xFE: // No expiry
                        keyLen = (int) readRdbLength(in);
                        key = readRdbString(in, keyLen);
                        valLen = (int) readRdbLength(in);
                        value = readRdbString(in, valLen);
                        map.put(key, new ValueWithExpiry(value, Long.MAX_VALUE));
                        break;
                    case 0xFF: // End of RDB file
                        return;
                    default:
                        // For simplicity, skip other opcodes
                        break;
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading RDB file: " + e.getMessage());
        }
    }

    private static long readRdbLength(InputStream in) throws IOException {
        int firstByte = in.read() & 0xFF;
        int type = (firstByte & 0xC0) >> 6;
        
        if (type == 0) {
            return firstByte & 0x3F;
        } else if (type == 1) {
            int secondByte = in.read() & 0xFF;
            return ((firstByte & 0x3F) << 8) | secondByte;
        } else if (type == 2) {
            byte[] bytes = new byte[4];
            in.read(bytes);
            return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL;
        } else {
            throw new IOException("Unsupported length encoding");
        }
    }

    private static String readRdbString(InputStream in, int length) throws IOException {
        byte[] bytes = new byte[length];
        in.read(bytes);
        return new String(bytes);
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

                        if (stored == null || (stored.expiryTimeMillis != Long.MAX_VALUE && 
                            System.currentTimeMillis() > stored.expiryTimeMillis)) {
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
                                outputStream.write("*0\r\n".getBytes());
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
                        outputStream.write("-ERR unknown command\r\n".getBytes());
                }
                outputStream.flush();
            }
        } catch (IOException e) {
            System.out.println("Client disconnected: " + e.getMessage());
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