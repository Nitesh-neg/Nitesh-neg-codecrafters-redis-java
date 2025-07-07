import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.file.*;
import java.util.*;

public class Main {
    
    static class ValueWithExpiry {
        String value;
        long expiryTime;
        
        ValueWithExpiry(String value, long expiryTime) {
            this.value = value;
            this.expiryTime = expiryTime;
        }
    }
    
    private static final Map<String, ValueWithExpiry> store = new HashMap<>();
    private static final Map<String, String> config = new HashMap<>();
    
    public static void main(String[] args) throws IOException {
        // Parse command line arguments
        parseArguments(args);
        
        // Load RDB file if specified
        if (config.containsKey("dir") && config.containsKey("dbfilename")) {
            Path rdbPath = Paths.get(config.get("dir"), config.get("dbfilename"));
            if (Files.exists(rdbPath)) {
                loadRdbFile(rdbPath);
            }
        }
        
        // Start server
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server listening on port " + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        }
    }
    
    private static void parseArguments(String[] args) {
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
            }
        }
    }
    
    private static void loadRdbFile(Path rdbPath) {
        try (DataInputStream in = new DataInputStream(Files.newInputStream(rdbPath))) {
            // Read and verify header
            byte[] header = new byte[9];
            in.readFully(header);
            if (!new String(header, 0, 5).equals("REDIS")) {
                throw new IOException("Invalid RDB file format");
            }
            
            // Skip metadata section (we don't need it for this stage)
            while (true) {
                int opcode = in.read();
                if (opcode == -1) break;
                
                if (opcode == 0xFA) { // Metadata subsection
                    skipString(in); // Skip key
                    skipString(in); // Skip value
                } else if (opcode == 0xFE) { // Database section
                    readDatabase(in);
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading RDB file: " + e.getMessage());
        }
    }
    
    private static void readDatabase(DataInputStream in) throws IOException {
        // Skip database index (we only support database 0)
        readLength(in);
        
        // Skip hash table sizes (we don't need them)
        if (in.read() == 0xFB) {
            readLength(in); // Skip keys size
            readLength(in); // Skip expires size
        }
        
        // Read key-value pairs
        while (true) {
            int opcode = in.read();
            if (opcode == -1 || opcode == 0xFF) break; // End of file
            
            long expiryTime = Long.MAX_VALUE;
            
            if (opcode == 0xFC) { // Expiry in milliseconds
                expiryTime = readUnsignedLong(in);
            } else if (opcode == 0xFD) { // Expiry in seconds
                expiryTime = readUnsignedInt(in) * 1000L;
            } else if (opcode != 0xFE && opcode != 0xFB && opcode != 0x00) {
                // Skip unsupported opcodes
                continue;
            }
            
            // Read value type (we only support strings in this stage)
            int valueType = in.read();
            if (valueType != 0) continue; // Skip non-string values
            
            // Read key and value
            String key = readString(in);
            String value = readString(in);
            
            store.put(key, new ValueWithExpiry(value, expiryTime));
        }
    }
    
    private static int readLength(DataInputStream in) throws IOException {
        int firstByte = in.read() & 0xFF;
        int type = (firstByte & 0xC0) >> 6;
        
        if (type == 0) {
            return firstByte & 0x3F;
        } else if (type == 1) {
            int secondByte = in.read() & 0xFF;
            return ((firstByte & 0x3F) << 8) | secondByte;
        } else if (type == 2) {
            return in.readInt();
        }
        throw new IOException("Unsupported length encoding");
    }
    
    private static long readUnsignedInt(DataInputStream in) throws IOException {
        byte[] bytes = new byte[4];
        in.readFully(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL;
    }
    
    private static long readUnsignedLong(DataInputStream in) throws IOException {
        byte[] bytes = new byte[8];
        in.readFully(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }
    
    private static String readString(DataInputStream in) throws IOException {
        int length = readLength(in);
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes);
    }
    
    private static void skipString(DataInputStream in) throws IOException {
        int length = readLength(in);
        in.skipBytes(length);
    }
    
    private static void handleClient(Socket clientSocket) {
        try (InputStream input = clientSocket.getInputStream();
             OutputStream output = clientSocket.getOutputStream()) {
            
            while (true) {
                List<String> command = parseCommand(input);
                if (command.isEmpty()) break;
                
                String cmd = command.get(0).toUpperCase();
                
                switch (cmd) {
                    case "GET":
                        handleGet(command, output);
                        break;
                    case "PING":
                        output.write("+PONG\r\n".getBytes());
                        break;
                    default:
                        output.write("-ERR unknown command\r\n".getBytes());
                }
                output.flush();
            }
        } catch (IOException e) {
            System.out.println("Client disconnected: " + e.getMessage());
        }
    }
    
    private static void handleGet(List<String> command, OutputStream output) throws IOException {
        if (command.size() < 2) {
            output.write("-ERR wrong number of arguments for 'get' command\r\n".getBytes());
            return;
        }
        
        String key = command.get(1);
        ValueWithExpiry entry = store.get(key);
        
        if (entry == null || (entry.expiryTime != Long.MAX_VALUE && 
            System.currentTimeMillis() > entry.expiryTime)) {
            output.write("$-1\r\n".getBytes());
        } else {
            String response = "$" + entry.value.length() + "\r\n" + entry.value + "\r\n";
            output.write(response.getBytes());
        }
    }
    
    private static List<String> parseCommand(InputStream input) throws IOException {
        DataInputStream in = new DataInputStream(input);
        List<String> command = new ArrayList<>();
        
        int b = in.read();
        if (b == -1) return command;
        if ((char) b != '*') throw new IOException("Expected RESP array");
        
        int numArgs = Integer.parseInt(readLine(in));
        for (int i = 0; i < numArgs; i++) {
            if (in.read() != '$') throw new IOException("Expected bulk string");
            int length = Integer.parseInt(readLine(in));
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            command.add(new String(bytes));
            readLine(in); // Discard CRLF
        }
        
        return command;
    }
    
    private static String readLine(DataInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            char c = (char) in.readByte();
            if (c == '\r') {
                if ((char) in.readByte() == '\n') break;
                sb.append('\r');
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}