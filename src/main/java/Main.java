import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.*;
import java.util.*;

public class Main {

    // Class to hold key-value pairs with expiry time

    static class ValueWithExpiry {
        String value;
        long expiryTimeMillis;

        ValueWithExpiry(String value, long expiryTimeMillis) {
            this.value = value;
            this.expiryTimeMillis = expiryTimeMillis;
        }
    }

    // Class that holds the command and the number of bytes consumed while parsing the RESP
    // This is used to keep track of how many bytes were read from the input stream
    // and to update the offset of the replica connection .

     static class ParseResult {
        List<String> command;
        int bytesConsumed;

        ParseResult(List<String> command, int bytesConsumed) {
            this.command = command;
            this.bytesConsumed = bytesConsumed;
        }
    }

    // class to hold the stream entries

    public static class StreamEntry {
        public String id;
        public Map<String, String> fields;

        public StreamEntry(String id, Map<String, String> fields) {
            this.id = id;
            this.fields = fields;
        }
    }



    public static long offset = 0;
    public static int replicaReadyForCommands = 0;

    // for storing replica connections output stream
    public static List<ReplicaConnection> replicaConnections = new ArrayList<>();

    // for storing the key-value pairs with expiry time
    public static final Map<String, ValueWithExpiry> map = new HashMap<>();

    // for storing the stream entries 
    public static final Map<String, List<StreamEntry>> streamMap = new HashMap<>();

    // replica that is connected to the master and received the command
    public static final Map<String, String> config = new HashMap<>();

    public static void main(String[] args) throws IOException {
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
                case "--port":
                    if (i + 1 < args.length) {
                        config.put("port", args[i + 1]);
                        i++;
                    }
                    break;
                case "--replicaof":
                    if (i + 1 < args.length) {
                        config.put("replicaof", args[i + 1]);
                        i++;
                    }
                    break;
                default:
                    break;
            }
        }

        int port = 6379;
        if (config.get("port") != null) {
            port = Integer.parseInt(config.get("port"));
            new Thread(ReplicaClient::connectToMaster).start();
        }

        if (config.get("dir") != null && config.get("dbfilename") != null) {
            final Path path = Paths.get(config.get("dir") + '/' + config.get("dbfilename"));
            final byte[] bytes;

            // Loading the RDB file if it exists
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
                    long expiryTime = Long.MAX_VALUE;
                    if (bytes[i] == (byte) 0xfc && i + 8 < bytes.length) {
                        final byte[] exp_byte = new byte[8];
                        for (int j = 0; j < 8; j++) {
                            exp_byte[j] = bytes[j + i + 1];
                        }
                        ByteBuffer buffer = ByteBuffer.wrap(exp_byte).order(ByteOrder.LITTLE_ENDIAN);
                        expiryTime = buffer.getLong();
                        i += 9;
                    }

                    if (bytes[i] == (byte) 0x00 && i + 1 < bytes.length) {
                        final int keyStrLen = bytes[i + 1] & 0xFF;
                        if (keyStrLen <= 0) continue;
                        final byte[] keyBytes = new byte[keyStrLen];
                        for (int j = i + 2; j < i + 2 + keyStrLen; j++) {
                            keyBytes[j - (i + 2)] = bytes[j];
                        }
                        i += 2 + keyStrLen;
                        if (i >= bytes.length) break;
                        final int valueStrLen = bytes[i] & 0xFF;
                        if (valueStrLen <= 0) continue;

                        final byte[] valueBytes = new byte[valueStrLen];
                        for (int j = i + 1; j < i + 1 + valueStrLen; j++) {
                            valueBytes[j - (i + 1)] = bytes[j];
                        }
                        map.put(new String(keyBytes), new ValueWithExpiry(new String(valueBytes), expiryTime));
                    }
                }
            } catch (IOException e) {
                System.out.println("RDB file not found or error reading it: " + e);
            }
        }

        // server socket accepting connections

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected");
                new Thread(() -> Utils.handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.out.println("Server error: " + e.getMessage());
        }
    }
  }