import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Utils {

    public static long master_offset = 0;
    public static Main.ParseResult prevCommand = null;
    public static Map<String, List<String>> rpushMap = new ConcurrentHashMap<>();
    public static boolean added_or_not = false;
    public static List<OutputStream> blocked = new ArrayList<>();


    public static void handleClient(Socket clientSocket) {

        // every client connection will gonna have their own transaction commands
        // that why we are creating a new List for each client connection and boolean variable

        boolean transactionStarted = false;
        List<List<String>> transactionCommands = new ArrayList<>();

        // for RPUSH to know if a list exists or not 
        // and the list 

      //  Map<String, List<String>> rpushMap = new ConcurrentHashMap<>();

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

                // if the transaction is started
                // then we will put SET , INCR, GET commands in the transactionCommands list
                // and we will not execute them immediately
                // we will execute them when EXEC command is received

                if(transactionStarted &&  (cmd.equals("SET") || cmd.equals("INCR") || cmd.equals("GET"))) {
                    transactionCommands.add(command);
                    outputStream.write("+QUEUED\r\n".getBytes());
                    outputStream.flush();
                    continue; // Skip further processing for this command
                }


                switch (cmd) {

                    // waiting for the replicas to acknowledge the command that are sent to them by the master (line SET key value)
                    // to make sure that the replicas are in sync with the master
                    // if the replicas are not in sync, then we will wait for the replicas to acknowledge

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

                    // putting the key-value pair in the map
                    // if the key already exists, it will be overwritten

                    case "SET":
                       String set_resp =  handleSetCommand(command, result.bytesConsumed);
                       outputStream.write(set_resp.getBytes("UTF-8"));
                       outputStream.flush();
                        break;

                    // getting the value of the key from the map

                    case "GET":
                        String get_resp = handleGetCommand(command);
                        outputStream.write(get_resp.getBytes("UTF-8"));
                        outputStream.flush();
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
                    // if the entryId is "*", it will be generated automatically

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
                    // if block is specified, it will wait until a new entry is added to the stream until the blocking time is reached


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
                        
                    // for increamting the value of a key by 1
                    // if the key does not exist, it will be created with the value 1
                    // if the value is not a valid integer, it will return an error
                    
                    case "INCR":
                          String resp_fromIncr = handleIncrCommand(command);
                          outputStream.write(resp_fromIncr.getBytes("UTF-8"));
                          outputStream.flush();
                        break;


                    case "MULTI":
                        // Start a transaction
                        transactionStarted = true;
                       // Main.transactionCommands = new ArrayList<>();
                        outputStream.write("+OK\r\n".getBytes());
                        outputStream.flush();
                        break;


                    case "EXEC":
                        
                       if(!transactionStarted){
                            outputStream.write("-ERR EXEC without MULTI\r\n".getBytes());
                            outputStream.flush();
                            break;
                       }
                       if(transactionCommands.isEmpty() && transactionStarted) {
                            outputStream.write("*0\r\n".getBytes());
                            outputStream.flush();
                            transactionStarted = false;
                            break;
                        }else{

                            StringBuilder execResponse = new StringBuilder();
                            execResponse.append("*").append(transactionCommands.size()).append("\r\n");

                            while(!transactionCommands.isEmpty()){
                                List<String> transactionCommand = transactionCommands.remove(0);
                                String transactionCmd = transactionCommand.get(0).toUpperCase();
                                String response ="";

                                if(transactionCmd.equals("SET")) {
                                    response = handleSetCommand(transactionCommand, 0);
                                } else if(transactionCmd.equals("INCR")) {
                                    response = handleIncrCommand(transactionCommand);
                                } else if(transactionCmd.equals("GET")) {
                                    response = handleGetCommand(transactionCommand);
                                } else {
                                    response = "-ERR Unsupported command in transaction\r\n";
                                }
                                execResponse.append(response);
                            }
                            outputStream.write(execResponse.toString().getBytes("UTF-8"));
                            outputStream.flush();
                            transactionStarted = false;
                            break;
                        }

                    // discard all the transactions commands that are queued
                    // clear the transactionCommands list and set transactionStarted to false

                    case "DISCARD":
                               if(transactionStarted){
                                    transactionCommands.clear();
                                    transactionStarted = false;
                                    outputStream.write("+OK\r\n".getBytes());
                                    outputStream.flush();
                                } else {
                                    outputStream.write("-ERR DISCARD without MULTI\r\n".getBytes());
                                    outputStream.flush();
                               } 
                               
                               break;

                    case "RPUSH":
                        String rpushKey = command.get(1);
                       // added_or_not = true;
                        if(rpushMap.containsKey(rpushKey)){
                            
                            int command_no = 2;
                            while( command_no < command.size()) {
                                rpushMap.get(rpushKey).add(command.get(command_no));
                                command_no++;
                            }

                            String respRpush = ":" + rpushMap.get(rpushKey).size() + "\r\n";
                            outputStream.write(respRpush.getBytes());
                            outputStream.flush();
                        }else {
                            List<String> newList = new ArrayList<>();
                            int command_no = 2;
                            while (command_no < command.size()) {
                                newList.add(command.get(command_no));
                                command_no++;
                            }
                            rpushMap.put(rpushKey, newList);

                            String respRpush = ":" + newList.size() + "\r\n";
                            outputStream.write(respRpush.getBytes("UTF-8"));
                            outputStream.flush();
                        }

                        break;

                    // for reading the elements from the list --> start index and end index will be given (both should be included)
                    
                    case "LRANGE":
                        
                        String lrangeKey = command.get(1);
                        int start = Integer.parseInt(command.get(2));
                        int end = Integer.parseInt(command.get(3));

                        List<String> lrangeList = rpushMap.get(lrangeKey);
                        if (lrangeList == null || lrangeList.isEmpty() ) {
                            outputStream.write("*0\r\n".getBytes("UTF-8"));
                            outputStream.flush();
                            break;
                        }

                        // Adjust negative indices
                        int size = lrangeList.size();
                        if (start < 0) start = size + start;
                        if (end < 0) end = size + end;
                        start = Math.max(0, start);
                        end = Math.min(size - 1, end);

                        if (start > end || start >= size) {
                            outputStream.write("*0\r\n".getBytes("UTF-8"));
                            outputStream.flush();
                            break;
                        }

                        StringBuilder lrangeResp = new StringBuilder();
                        lrangeResp.append("*").append(end - start + 1).append("\r\n");
                        for (int i = start; i <= end; i++) {
                            String item = lrangeList.get(i);
                            lrangeResp.append("$").append(item.length()).append("\r\n").append(item).append("\r\n");
                        }

                        outputStream.write(lrangeResp.toString().getBytes("UTF-8"));
                        outputStream.flush();
                        break;

                    // insert the element at the start --> index 0

                    case "LPUSH":

                        key = command.get(1);
                        List<String> list = rpushMap.getOrDefault(key, new ArrayList<>());
                        for (int i = 2; i < command.size(); i++) {
                            list.add(0, command.get(i));  // insert at beginning
                        }
                        rpushMap.put(key, list);
                        String response = ":" + list.size() + "\r\n";
                        outputStream.write(response.getBytes("UTF-8"));
                        outputStream.flush();
                        break;

                    // for getting the length of the list
                    // if not exist , then send 0 

                    case "LLEN":

                        key = command.get(1);
                        if(rpushMap.containsKey(key)){
                            String resp_list = ":"+ rpushMap.get(key).size()+"\r\n";
                            outputStream.write(resp_list.getBytes());
                            outputStream.flush();
                        }else{
                            outputStream.write(":0\r\n".getBytes());
                            outputStream.flush();
                        }
                        break;

                    // removes the first element , then returns it as a response
                    // can also removes multiple elements--> for this you should send the number of elements that needed to be removed;
                    
                    case "LPOP":
                        key = command.get(1);
                        int count = 1;

                        if (command.size() >= 3) {
                            count = Integer.parseInt(command.get(2));
                        }

                        List<String> lpopList = rpushMap.get(key);

                        if (lpopList == null || lpopList.isEmpty()) {
                            outputStream.write("$-1\r\n".getBytes("UTF-8"));
                            outputStream.flush();
                            break;
                        }

                        //  to make sure that the number of elements that are removed should not be out of bound
                        count = Math.min(count, lpopList.size());

                        if (command.size() < 3) {
                            // when we have to delete single element
                            String poppedLpop = lpopList.remove(0);
                            String respLpop = "$" + poppedLpop.length() + "\r\n" + poppedLpop + "\r\n";
                            outputStream.write(respLpop.getBytes("UTF-8"));
                        } else {
                            //  when we have to delete multiple elements
                            StringBuilder respArray = new StringBuilder();
                            respArray.append("*").append(count).append("\r\n");

                            for (int i = 0; i < count; i++) {
                                String poppedLpop = lpopList.remove(0);
                                respArray.append("$").append(poppedLpop.length()).append("\r\n").append(poppedLpop).append("\r\n");
                            }

                            outputStream.write(respArray.toString().getBytes("UTF-8"));
                        }

                        outputStream.flush();
                        break;

                    case "BLPOP":

                        String blpopKey = command.get(1);
                        long deadline = 0;
                        boolean command_2_is_zero = true;

                        if(!command.get(2).equals("0")){
                            double timeoutSeconds = Double.parseDouble(command.get(2));
                            long timeoutMs = (long) (timeoutSeconds * 1000);
                            deadline = System.currentTimeMillis() + timeoutMs;

                            command_2_is_zero = false;
                        }

                        blocked.add(outputStream);
                        boolean element_is_appended = false;

                        while (command_2_is_zero || System.currentTimeMillis() < deadline) {
                             
                          //  System.out.println("***** inside while loop *********");

                           // while(added_or_not){
                            List<String> blpopList = rpushMap.get(blpopKey);
                            if (blpopList != null && !blpopList.isEmpty()) {

                                System.out.println("**  list is inside ***");

                                String poppedElement = blpopList.remove(0);
                                StringBuilder respBlpop = new StringBuilder();

                                System.out.println(poppedElement + " its this string");

                                respBlpop.append("*2\r\n");
                                respBlpop.append("$").append(blpopKey.length()).append("\r\n").append(blpopKey).append("\r\n");
                                respBlpop.append("$").append(poppedElement.length()).append("\r\n").append(poppedElement).append("\r\n");

                                OutputStream out = blocked.get(0);
                                
                                System.out.println(" this is outputStream : "+ out);
                                System.out.println("*** list is here ***");
                                element_is_appended = true;

                                out.write(respBlpop.toString().getBytes("UTF-8"));
                                out.flush();
                                break;
                           // }
                          //  blocked.add(outputStream);
                        }
                            
                        //     try {
                        //         Thread.sleep(10);
                        //     } catch (InterruptedException ignored) {}
                        // }
                        }

                        if(!element_is_appended){
                            blocked.remove(0);
                            outputStream.write("$-1\r\n".getBytes());
                            outputStream.flush();
                        }
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

    private static String handleIncrCommand(List<String> command) {

        String incrKey = command.get(1);
        Main.ValueWithExpiry incrValue = Main.map.get(incrKey);
        long increment = 1;

        if (incrValue != null) {
            try {
                long currentValue = Long.parseLong(incrValue.value); // convert a valid integer string to long
                long newValue = currentValue + increment;
                incrValue.value = String.valueOf(newValue);
                return ":" + newValue + "\r\n";
            } catch (NumberFormatException e) {
                return "-ERR value is not an integer or out of range\r\n";
            }
        } else {
            incrValue = new Main.ValueWithExpiry(String.valueOf(increment), Long.MAX_VALUE);
            Main.map.put(incrKey, incrValue);
            return ":" + increment + "\r\n";
        }
    }

    private static String handleSetCommand(List<String> command, int bytesConsumed) throws IOException {

        String key = command.get(1);
        String value = command.get(2);
        long expiryTime = Long.MAX_VALUE;

        if (command.size() >= 5 && command.get(3).equalsIgnoreCase("PX")) {
            try {
                long pxMillis = Long.parseLong(command.get(4));
                expiryTime = System.currentTimeMillis() + pxMillis;
            } catch (NumberFormatException e) {
                return ("-ERR invalid PX value\r\n");
            }
        }

        master_offset += bytesConsumed;

        Main.map.put(key, new Main.ValueWithExpiry(value, expiryTime));
        // outputStream.write("+OK\r\n".getBytes());
        // outputStream.flush();

        for (ReplicaConnection replica : Main.replicaConnections) {
            replica.getOutputStream().write(buildRespArray("SET", key, value).getBytes());
            replica.getOutputStream().flush();
            
            replica.setOffset(bytesConsumed);
        }

        return "+OK\r\n";

    }

    private static String handleGetCommand(List<String> command) throws IOException {
        String key = command.get(1);
        Main.ValueWithExpiry stored = Main.map.get(key);

        if (stored == null || (stored.expiryTimeMillis != 0 && System.currentTimeMillis() > stored.expiryTimeMillis)) {
            Main.map.remove(key);
           // outputStream.write("$-1\r\n".getBytes());
           return "$-1\r\n";
        } else {
            String resp = "$" + stored.value.length() + "\r\n" + stored.value + "\r\n";
           // outputStream.write(resp.getBytes());
           return resp;
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

        // xread with block command with variable blocking time
        // if the blocking time is 0, it will wait until a new entry is added to the stream


    public static void blockingRead(List<String> command, OutputStream outputStream) throws IOException {
        long blockMs = Long.parseLong(command.get(2));
        boolean new_entry_added=true;

        // if the bocking time is 0,
        if(blockMs == 0){
            new_entry_added = false; // If blockMs is 0

        }
        long deadline = System.currentTimeMillis() + blockMs;

        String streamKey = command.get(4);
        String startId = command.get(5);
       // String for_dollar = command.get(5);
       if(startId.equals("$")) {
            List<Main.StreamEntry> entries = Main.streamMap.get(streamKey);
            startId = entries.get(entries.size() - 1).id;  // Use last entry ID if "+" is specified
                        
        }
        boolean responseFound = false;

        while (System.currentTimeMillis() < deadline || !new_entry_added) {

            List<Main.StreamEntry> entries = Main.streamMap.get(streamKey);
           
            StringBuilder resp = new StringBuilder();
           // System.out.println("********************");

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
                          //  new_entry_added = true; // We found a new entry
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
