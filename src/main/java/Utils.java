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
                    
                        // reading the entries from the stream , startId and endId are included in the range
                        // if the startId is "-", it means the startId is the first entry in the stream
                        // if the endId is "+", it means the endId is the last entry in the stream

                    case "XRANGE":
                        String stream = command.get(1);
                        String startId = command.get(2);
                        String endId = command.get(3);

                        List<Main.StreamEntry> streamEntries = Main.streamMap.get(stream);

                        if (streamEntries == null || streamEntries.isEmpty()) {
                            outputStream.write("*0\r\n".getBytes("UTF-8"));
                            outputStream.flush();
                            break;
                        }

                        // endId handling

                        if(endId.equals("+")) {
                            endId = streamEntries.get(streamEntries.size() - 1).id;  // Use last entry ID if "+" is specified
                        }

                        StringBuilder respStream = new StringBuilder();
                        List<String> matched = new ArrayList<>();

                        for (Main.StreamEntry streamEntry : streamEntries) {

                            // startId = "-" , then we will give the first entry in the stream

                             if(startId.equals("-")){
                                startId =  streamEntry.id;
                             }

                            if (isInRange(streamEntry.id, startId, endId)) {
                                StringBuilder entryResp = new StringBuilder();
                                entryResp.append("*2\r\n"); // one entry = [id, fields-array]
                                entryResp.append("$").append(streamEntry.id.length()).append("\r\n")
                                        .append(streamEntry.id).append("\r\n");

                                entryResp.append("*").append(streamEntry.fields.size() * 2).append("\r\n");
                                for (Map.Entry<String, String> field : streamEntry.fields.entrySet()) {
                                    entryResp.append("$").append(field.getKey().length()).append("\r\n")
                                            .append(field.getKey()).append("\r\n");
                                    entryResp.append("$").append(field.getValue().length()).append("\r\n")
                                            .append(field.getValue()).append("\r\n");
                                }

                                matched.add(entryResp.toString());
                            }
                        }

                        respStream.append("*").append(matched.size()).append("\r\n");
                        for (String m : matched) respStream.append(m);

                        outputStream.write(respStream.toString().getBytes("UTF-8"));
                        outputStream.flush();
                        break;

                    // readin the entires , greater than the given streamId

                  case "XREAD":
                        List<String> streamKeys = new ArrayList<>();
                        List<String> streamIds = new ArrayList<>();

                        if(command.get(1).equals("block")){
                            // Handle blocking read
                            //long deadline = Integer.parseInt(command.get(2)) +  System.currentTimeMillis();

                            blockingRead(command, outputStream);
                            break;
                        }

                        if (command.size() >= 6) {
                            // Multiple streams
                            streamKeys.add(command.get(2));
                            streamKeys.add(command.get(3));
                            streamIds.add(command.get(4));
                            streamIds.add(command.get(5));
                        } else {
                            // Single stream
                            streamKeys.add(command.get(2));
                            streamIds.add(command.get(3));
                        }

                        StringBuilder fullResponse = new StringBuilder();
                        List<String> allStreamsOutput = new ArrayList<>();

                        for (int i = 0; i < streamKeys.size(); i++) {
                            String xreadStreamKey = streamKeys.get(i);
                            String xreadStartId = streamIds.get(i);

                            List<Main.StreamEntry> xreadEntries = Main.streamMap.get(xreadStreamKey);
                            if (xreadEntries == null || xreadEntries.isEmpty()) {
                                continue;  // no entries for this stream
                            }

                            List<String> xreadMatched = new ArrayList<>();
                            for (Main.StreamEntry xreadEntry : xreadEntries) {
                                if (compareIds(xreadEntry.id, xreadStartId) > 0) {
                                    StringBuilder entryResp = new StringBuilder();
                                    entryResp.append("*2\r\n"); // [id, [fields...]]
                                    entryResp.append("$").append(xreadEntry.id.length()).append("\r\n")
                                            .append(xreadEntry.id).append("\r\n");

                                    entryResp.append("*").append(xreadEntry.fields.size() * 2).append("\r\n");
                                    for (Map.Entry<String, String> field : xreadEntry.fields.entrySet()) {
                                        entryResp.append("$").append(field.getKey().length()).append("\r\n")
                                                .append(field.getKey()).append("\r\n");
                                        entryResp.append("$").append(field.getValue().length()).append("\r\n")
                                                .append(field.getValue()).append("\r\n");
                                    }
                                    xreadMatched.add(entryResp.toString());
                                }
                            }

                            if (!xreadMatched.isEmpty()) {
                                StringBuilder streamBlock = new StringBuilder();
                                streamBlock.append("*2\r\n");
                                streamBlock.append("$").append(xreadStreamKey.length()).append("\r\n")
                                        .append(xreadStreamKey).append("\r\n");
                                streamBlock.append("*").append(xreadMatched.size()).append("\r\n");
                                for (String e : xreadMatched) streamBlock.append(e);

                                allStreamsOutput.add(streamBlock.toString());
                            }
                        }

                        if (allStreamsOutput.isEmpty()) {
                            outputStream.write("*0\r\n".getBytes("UTF-8"));
                        } else {
                            fullResponse.append("*").append(allStreamsOutput.size()).append("\r\n");
                            for (String streamResp : allStreamsOutput) {
                                fullResponse.append(streamResp);
                            }
                            outputStream.write(fullResponse.toString().getBytes("UTF-8"));
                        }

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

    private static boolean isInRange(String id, String start, String end) {
            return compareIds(id, start) >= 0 && compareIds(id, end) <= 0;
        }

    private static int compareIds(String id1, String id2) {
            String[] p1 = id1.split("-");
            String[] p2 = id2.split("-");
            long t1 = Long.parseLong(p1[0]);
            long s1 = Long.parseLong(p1[1]);
            long t2 = Long.parseLong(p2[0]);
            long s2 = Long.parseLong(p2[1]);

            if (t1 != t2) return Long.compare(t1, t2);
            return Long.compare(s1, s2);
        }

    public static void blockingRead(List<String> command, OutputStream outputStream) throws IOException {
        long blockMs = Long.parseLong(command.get(2));
        long deadline = System.currentTimeMillis() + blockMs;

        String streamKey = command.get(4);
        String startId = command.get(5);
        boolean responseFound = false;

        while (System.currentTimeMillis() < deadline) {
            List<Main.StreamEntry> entries = Main.streamMap.get(streamKey);
            StringBuilder resp = new StringBuilder();
            System.out.println("********************");

            if (entries != null && !entries.isEmpty()) {
                for (Main.StreamEntry entry : entries) {
                    if (compareIds(entry.id, startId) > 0) {
                        // RESP format:
                        resp.append("*1\r\n"); // one stream
                        resp.append("*2\r\n"); // [key, [entry]]
                        resp.append("$").append(streamKey.length()).append("\r\n").append(streamKey).append("\r\n");

                        resp.append("*1\r\n"); // one matching entry
                        resp.append("*2\r\n"); // [id, [fields]]
                        resp.append("$").append(entry.id.length()).append("\r\n").append(entry.id).append("\r\n");

                        resp.append("*").append(entry.fields.size() * 2).append("\r\n");
                        for (Map.Entry<String, String> field : entry.fields.entrySet()) {
                            resp.append("$").append(field.getKey().length()).append("\r\n").append(field.getKey()).append("\r\n");
                            resp.append("$").append(field.getValue().length()).append("\r\n").append(field.getValue()).append("\r\n");
                        }

                        if(!resp.isEmpty()){

                            responseFound = true;
                        } 
                    }
                }
            }

            if(responseFound) {
                outputStream.write(resp.toString().getBytes("UTF-8"));
                outputStream.flush();
                return;  // Exit if we found a response
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {}
        }

        // If no new entries found after deadline
        outputStream.write("$-1\r\n".getBytes("UTF-8"));
        outputStream.flush();
    }

}
