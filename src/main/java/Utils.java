import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {

    public static long master_offset = 0;
    public static Main.ParseResult prevCommand = null;

    public static void handleClient(Socket clientSocket) {

        try (
            Socket socket = clientSocket;
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream()
        ) {
            while (true) {

                Main.ParseResult result = RESPParser.parseRESP(inputStream);
                List<String> command = result.command;
                if (command.isEmpty()) continue;

                System.out.println("Parsed RESP command: " + command);
                String cmd = command.get(0).toUpperCase();

                switch (cmd) {

                    // waiting for the replicas to acknowledge the command that are sent to them by the master (line SET key value)

                    case "WAIT":      
                                
                            long currentMasterOffset =  master_offset;
                            int required_replica=Integer.parseInt(command.get(1));
                            int timeout = Integer.parseInt(command.get(2));
                            System.out.println("entered wait conditon");
                            System.out.println(master_offset);
                            int replicasAcked = ReplicaAckWaiter.waitForAcks(required_replica,timeout, currentMasterOffset);// required , timeout, masteroffset
                            if(master_offset == 0) {
                                replicasAcked = Main.replicaConnections.size();
                            }
                            String resp1 = ":" + replicasAcked + "\r\n";
                            outputStream.write(resp1.getBytes("UTF-8"));
                            outputStream.flush();

                        break;

                    case "PING":
                        outputStream.write("+PONG\r\n".getBytes());
                        outputStream.flush();
                        break;

                    case "ECHO":
                        String echoMsg = command.get(1);
                        String resp = "$" + echoMsg.length() + "\r\n" + echoMsg + "\r\n";
                        outputStream.write(resp.getBytes());
                        outputStream.flush();
                        break;

                    case "SET":
                        handleSetCommand(command, result.bytesConsumed, outputStream);
                        break;

                    case "GET":
                        handleGetCommand(command, outputStream);
                        break;

                    case "CONFIG":
                        handleConfigCommand(command, outputStream);
                        break;

                    case "REPLCONF":
                        if (command.size() >= 3 && command.get(1).equalsIgnoreCase("ACK")) {
                            ReplicaConnection replica = Utils.findReplicaBySocket(socket);
                            if (replica != null) {
                                long replicaOffset = Long.parseLong(command.get(2));
                                replica.setOffset(replicaOffset);
                                replica.setack(true);
                            }
                            break;
                        }

                        if (!command.get(1).equalsIgnoreCase("GETACK")) {
                            outputStream.write("+OK\r\n".getBytes());
                            outputStream.flush();
                            break;
                        }
                        // Else: GETACK — do nothing, wait for ACK
                        break;

                    case "PSYNC":
                        handlePsyncCommand(socket, inputStream, outputStream);
                        break;

                    // finding all the keys stored in the database

                    case "KEYS":
                        handleKeysCommand(command, outputStream);
                        break;

                    case "INFO":
                        handleInfoCommand(command, outputStream);
                        break;

                    // finding the type of value stored in the key
                   case "TYPE":
                        String key = command.get(1);
                        if (Main.map.containsKey(key)) {
                            outputStream.write("+string\r\n".getBytes());
                        } else if (Main.streamMap.containsKey(key)) {
                            outputStream.write("+stream\r\n".getBytes());
                        } else {
                            outputStream.write("+none\r\n".getBytes());
                        }
                        outputStream.flush();
                        break;

                    // adding a new entry to the stream

                   case "XADD":
                        String streamKey = command.get(1);
                        String entryId = command.get(2);
                    
                        Map<String, String> fields = new HashMap<>();
                        for (int i = 3; i < command.size(); i += 2) {
                            fields.put(command.get(i), command.get(i + 1));
                        }

                        if(entryId.equals("*")) {
                            entryId = System.currentTimeMillis() + "-0";  // Default to current timestamp and sequence 0
                        }

                        // Parse entry ID
                        String[] parts = entryId.split("-");
                        if (parts.length != 2) {
                            outputStream.write("-ERR Invalid entry ID format\r\n".getBytes("UTF-8"));
                            outputStream.flush();
                            break;
                        }

                        long ts = Long.parseLong(parts[0]);
                        long seq;

                        if (parts[1].equals("*")) {
                            seq = 0;

                            List<Main.StreamEntry> entries = Main.streamMap.getOrDefault(streamKey, new ArrayList<>());
                            for (int i = entries.size() - 1; i >= 0; i--) {
                                String[] lastParts = entries.get(i).id.split("-");
                                long lastTs = Long.parseLong(lastParts[0]);
                                long lastSeq = Long.parseLong(lastParts[1]);
                                if (lastTs == ts) {
                                    seq = lastSeq + 1;
                                    break;
                                }
                            }

                            entryId = ts + "-" + seq;  //  IMPORTANT: overriding entryId now
                        } else {
                            seq = Long.parseLong(parts[1]);
                        }

                        if(ts == 0 && parts[1].equals("*")  && seq == 0) {
                            seq = 1;  // Default to 1 if no sequence provided
                            entryId = ts + "-1";  
                        }

                        if (ts == 0 && seq == 0) {
                            outputStream.write("-ERR The ID specified in XADD must be greater than 0-0\r\n".getBytes("UTF-8"));
                            outputStream.flush();
                            break;
                        }

                        // Reject duplicate/smaller IDs
                        List<Main.StreamEntry> entries = Main.streamMap.getOrDefault(streamKey, new ArrayList<>());
                        if (!entries.isEmpty()) {
                            String[] lastParts = entries.get(entries.size() - 1).id.split("-");
                            long lastTs = Long.parseLong(lastParts[0]);
                            long lastSeq = Long.parseLong(lastParts[1]);

                            if (ts < lastTs || (ts == lastTs && seq <= lastSeq)) {
                                outputStream.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes("UTF-8"));
                                outputStream.flush();
                                break;
                            }
                        }

                        // Add to stream
                        Main.StreamEntry entry = new Main.StreamEntry(entryId, fields);
                        Main.streamMap.computeIfAbsent(streamKey, k -> new ArrayList<>()).add(entry);

                       
                        String respBulk = "$" + entryId.length() + "\r\n" + entryId + "\r\n";
                        outputStream.write(respBulk.getBytes("UTF-8"));
                        outputStream.flush();
                        break;



                    default:
                        outputStream.write("- unknown command\r\n".getBytes());
                        break;
                }
            }

        } catch (IOException e) {
            System.out.println("Client disconnected or error: " + e.getMessage());
        }
    }

    private static void handleSetCommand(List<String> command, int bytesConsumed, OutputStream outputStream) throws IOException {

        String key = command.get(1);
        String value = command.get(2);
        long expiryTime = Long.MAX_VALUE;

        if (command.size() >= 5 && command.get(3).equalsIgnoreCase("PX")) {
            try {
                long pxMillis = Long.parseLong(command.get(4));
                expiryTime = System.currentTimeMillis() + pxMillis;
            } catch (NumberFormatException e) {
                outputStream.write("-ERR invalid PX value\r\n".getBytes());
                return;
            }
        }

        master_offset += bytesConsumed;
        Main.map.put(key, new Main.ValueWithExpiry(value, expiryTime));
        outputStream.write("+OK\r\n".getBytes());
        outputStream.flush();

        for (ReplicaConnection replica : Main.replicaConnections) {
            replica.getOutputStream().write(buildRespArray("SET", key, value).getBytes());
            replica.getOutputStream().flush();
            
            replica.setOffset(bytesConsumed);
        }
    }

    private static void handleGetCommand(List<String> command, OutputStream outputStream) throws IOException {
        String key = command.get(1);
        Main.ValueWithExpiry stored = Main.map.get(key);

        if (stored == null || (stored.expiryTimeMillis != 0 && System.currentTimeMillis() > stored.expiryTimeMillis)) {
            Main.map.remove(key);
            outputStream.write("$-1\r\n".getBytes());
        } else {
            String resp = "$" + stored.value.length() + "\r\n" + stored.value + "\r\n";
            outputStream.write(resp.getBytes());
        }
    }

    private static void handleConfigCommand(List<String> command, OutputStream outputStream) throws IOException {
        if (command.size() >= 3 && command.get(1).equalsIgnoreCase("GET")) {
            String key = command.get(2);
            String value = Main.config.get(key);
            if (value != null) {
                String resp = "*2\r\n" +
                              "$" + key.length() + "\r\n" + key + "\r\n" +
                              "$" + value.length() + "\r\n" + value + "\r\n";
                outputStream.write(resp.getBytes());
                outputStream.flush();
            } else {
                outputStream.write("*0\r\n".getBytes()); // RESP empty array
                outputStream.flush();
            }
        } else {
            outputStream.write("-ERR wrong CONFIG usage\r\n".getBytes());
            outputStream.flush();
        }
    }

       private static void handlePsyncCommand(Socket socket, InputStream inputStream, OutputStream outputStream) throws IOException {

        String replicationId = "0123456789abcdef0123456789abcdef012345670";
        long offset = 0;
        String reply = "+FULLRESYNC " + replicationId + " " + offset + "\r\n";
        outputStream.write(reply.getBytes());
        outputStream.flush();

        String base64RDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        byte[] rdbBytes = Base64.getDecoder().decode(base64RDB);

        outputStream.write(("$" + rdbBytes.length + "\r\n").getBytes());
        outputStream.flush();
        outputStream.write(rdbBytes);
        outputStream.flush();

        // Store replica connection for future writes:
        Main.replicaConnections.add(new ReplicaConnection(socket, inputStream, outputStream));
     }

    private static void handleKeysCommand(List<String> command, OutputStream outputStream) throws IOException {

        if (command.get(1).equals("*")) {
            StringBuilder respKeys = new StringBuilder();
            respKeys.append("*").append(Main.map.size()).append("\r\n");
            for (String key : Main.map.keySet()) {
                respKeys.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
            }
            outputStream.write(respKeys.toString().getBytes());
            outputStream.flush();
        }
    }

    private static void handleInfoCommand(List<String> command, OutputStream outputStream) throws IOException {

        if (command.get(1).equals("replication") && Main.config.get("replicaof") == null) {
            StringBuilder info = new StringBuilder();
            info.append("role:master\n")
                .append("master_replid:0123456789abcdef0123456789abcdef01234567\n")
                .append("master_repl_offset:0\n");
            String resp = "$" + info.length() + "\r\n" + info + "\r\n";
            outputStream.write(resp.getBytes());
            outputStream.flush();
        } else {
            String slaveResp = "$" + "role:slave".length() + "\r\n" + "role:slave" + "\r\n";
            outputStream.write(slaveResp.getBytes());
            outputStream.flush();
        }
    }

    public static void skipUntilStar(PushbackInputStream pin) throws IOException {

        int b;
        while ((b = pin.read()) != -1) {
            if (b == '*') {
                System.out.println("Found '*', pushing back to stream...");
                pin.unread(b);
                return;
            }
        }
        throw new EOFException("Reached end of stream before finding '*'");
    }

    public static String buildRespArray(String... args) {

        StringBuilder sb = new StringBuilder();
        sb.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            sb.append("$").append(arg.length()).append("\r\n").append(arg).append("\r\n");
        }
        return sb.toString();
    }

    public static ReplicaConnection findReplicaBySocket(Socket socket) {
        for (ReplicaConnection r : Main.replicaConnections) {
            if (r.getSocket().equals(socket)) return r;
        }
        return null;
    }

}
