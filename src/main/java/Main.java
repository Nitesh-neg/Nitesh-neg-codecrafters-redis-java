import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
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

    static InputStream masterStream=null;

    public static int replicaReadyForCommands=0;

    private static List<OutputStream> replicaConnections = new ArrayList<>();

    private static final Map<String, ValueWithExpiry> map = new HashMap<>();
    private static final Map<String, String> config = new HashMap<>();

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
                    if(i+1< args.length){
                        config.put("--port",args[i+1]);
                    }
                    break;

                case "--replicaof":
                       if(i+1<args.length){
                            config.put("--replicaof", args[i+1]);
                       }
                       break;

                default:
                    break;
            }
        }

        int port = 6379;
        if(config.get("--port")!=null){

            port=Integer.parseInt(config.get("--port"));
            new Thread(Main::connectToMaster).start();
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

                for (int i = databaseSectionOffset + 4; i < bytes.length; i++) {  // reading the rdf file 

                    long expiryTime=Long.MAX_VALUE;

                    if(bytes[i]==(byte) 0xfc && i+8 < bytes.length){

                        final byte[] exp_byte=new byte[8];
                        for(int j=0;j<8;j++){
                            
                            exp_byte[j]=bytes[j+i+1];
                             
                        }
                        ByteBuffer buffer = ByteBuffer.wrap(exp_byte)
                                     .order(ByteOrder.LITTLE_ENDIAN);
                                     expiryTime = buffer.getLong();
                                     i+=9;
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
                        map.put(
                                new String(keyBytes),
                                new ValueWithExpiry(new String(valueBytes), expiryTime));
                    }
                }

            } catch (IOException e) {
                System.out.println("RDB file not found or error reading it: " + e);
                // Continue with empty DB
            }
        }

        try (ServerSocket serverSocket = new ServerSocket(port)) {  // mutliple clients
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

        static void connectToMaster() {   // replica 
            try {

                String[] parts = config.get("--replicaof").split(" ");
                String masterHost = (parts[0]).toString();
                int masterPort = Integer.parseInt(parts[1]);


                Socket socket = new Socket(masterHost, masterPort);
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                byte[] buffer = new byte[1024];


                String pingCommand = "*1\r\n$4\r\nPING\r\n";
                out.write(pingCommand.getBytes());
                out.flush();

                int bytesRead = in.read(buffer);
                // if (bytesRead == -1) {
                //  //   socket.close();
                //     return;
                //     }
                
                String replconf1Resp =
                            buildRespArray("REPLCONF", "listening-port", String.valueOf(config.get("--port")));
                    out.write(replconf1Resp.getBytes());
                    out.flush();

                bytesRead = in.read(buffer);
                // if (bytesRead == -1) {
                //  //   socket.close();
                //     return;
                //     }

                String reply = new String(buffer, 0, bytesRead).trim();
                // if (!reply.equals("+OK")) {
                //     //    socket.close();
                //         return;
                //     }
                
                String replconf2Resp = buildRespArray("REPLCONF", "capa","psync2");
                    out.write(replconf2Resp.getBytes());
                    out.flush();

                bytesRead = in.read(buffer);
                    // if (bytesRead == -1) {
                    // //    socket.close();
                    //     return;
                    // }

               reply = new String(buffer, 0, bytesRead).trim();
                //   if (!reply.equals("+OK")) {
                //       //  socket.close();
                //         return;
                //     }
                
                String psync =buildRespArray("PSYNC","?","-1");
                out.write(psync.getBytes());
                out.flush();
                
                bytesRead=in.read(buffer);
                    // if(bytesRead ==-1){
                    //  //   socket.close();
                    //     return;
                    // }
                
                reply = new String(buffer,0,bytesRead).trim();
                // if(!reply.equals("+FULLRESYNC")){
                //     return;
                // }

                while (true) {
                List<String> command = parseRESP(in);
                if (command.isEmpty()) continue;

                System.out.println("Parsed RESP command: " + command);
                System.out.flush();
                String cmd = command.get(0).toUpperCase();


                 switch (cmd) {

                    case "SET":
                        String key = command.get(1);
                        String value = command.get(2);
                        long expiryTime = Long.MAX_VALUE;

                        if (command.size() >= 5 && command.get(3).equalsIgnoreCase("PX")) {
                            try {
                                long pxMillis = Long.parseLong(command.get(4));
                                expiryTime = System.currentTimeMillis() + pxMillis;
                            } catch (NumberFormatException e) {
                                out.write("-ERR invalid PX value\r\n".getBytes());
                                continue;
                            }
                        }

                        map.put(key, new ValueWithExpiry(value, expiryTime));
                        break;
                    
                    case "REPLCONF":
                          if (command.size() >= 3 &&
                                command.get(0).equalsIgnoreCase("REPLCONF") &&
                                command.get(1).equalsIgnoreCase("GETACK")) {
                                
                                String reply_1 = "REPLCONF ACK 0\r\n";
                                masterStream.read(reply_1.getBytes());
                            }   

                    default:
                         break;
                    }
                }
            }      

            catch (IOException e) {

                System.out.println("Replica connection error: " + e.getMessage());
            }

        }   

        static String buildRespArray(String... args) {
                StringBuilder sb = new StringBuilder();

                sb.append("*").append(args.length).append("\r\n");

                for (String arg : args) {
                
                    sb.append("$").append(arg.length()).append("\r\n");
                    sb.append(arg).append("\r\n");
                }

                return sb.toString();
            }



    private static void handleClient(Socket clientSocket) {
        try (Socket socket = clientSocket;
                InputStream inputStream = socket.getInputStream();
                OutputStream outputStream = socket.getOutputStream()) {
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
                        outputStream.flush();


                       for (OutputStream replicaOutputStream : replicaConnections) {
                        replicaOutputStream.write(buildRespArray("SET", key, value).getBytes());
                        replicaOutputStream.write("*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n".getBytes());
                    }                

                        break;

                    case "GET":
                        String getKey = command.get(1);
                        ValueWithExpiry stored = map.get(getKey);

                        if (stored == null
                                || (stored.expiryTimeMillis != 0
                                        && System.currentTimeMillis() > stored.expiryTimeMillis)) {
                            map.remove(getKey);
                            outputStream.write("$-1\r\n".getBytes());
                        } else {
                            String getResp =
                                    "$" + stored.value.length() + "\r\n" + stored.value + "\r\n";
                            outputStream.write(getResp.getBytes());
                        }
                        break;

                    case "CONFIG":
                        if (command.size() >= 3 && command.get(1).equalsIgnoreCase("GET")) {
                            String key_1 = command.get(2);
                            String value_1 = config.get(key_1);
                            if (value_1 != null) {
                                String respConfig =
                                        "*2\r\n"
                                                + "$"
                                                + key_1.length()
                                                + "\r\n"
                                                + key_1
                                                + "\r\n"
                                                + "$"
                                                + value_1.length()
                                                + "\r\n"
                                                + value_1
                                                + "\r\n";
                                outputStream.write(respConfig.getBytes());
                            } else {
                                outputStream.write("*0\r\n".getBytes()); // RESP empty array
                            }
                        }
                         else {
                            outputStream.write("-ERR wrong CONFIG usage\r\n".getBytes());
                        }
                        break;

                    case "REPLCONF":
                                  outputStream.write("+OK\r\n".getBytes());
                                  break;

                    case "PSYNC" : 
                                    String replicationId = "0123456789abcdef0123456789abcdef012345670";
                                    long offset = 0;
                                    String reply = "+FULLRESYNC " + replicationId + " " + offset + "\r\n";
                                    outputStream.write(reply.getBytes());
                                    outputStream.flush();

                                    String base64RDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="; 

                                    byte[] rdbBytes = Base64.getDecoder().decode(base64RDB);

                                    outputStream.write(("$" + rdbBytes.length + "\r\n").getBytes());
                                    outputStream.flush();

                                    masterStream=inputStream;

                                    outputStream.write(rdbBytes);
                                    replicaConnections.add(outputStream); // tcp connections --> so that later master can update the data on replica side.
                                    outputStream.flush();

                                     for (OutputStream replicaOutputStream : replicaConnections) {
                                                replicaOutputStream.write("*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n".getBytes());
                                            }     

         

                                    break;

                    case "KEYS":
                        if (command.get(1).equals("*")) {
                            StringBuilder respKeys = new StringBuilder();
                            respKeys.append("*").append(map.size()).append("\r\n");
                            for (String key_2 : map.keySet()) {
                                respKeys.append("$")
                                        .append(key_2.length())
                                        .append("\r\n")
                                        .append(key_2)
                                        .append("\r\n");
                            }
                            outputStream.write(respKeys.toString().getBytes());
                        }
                        break;

                    case "INFO":
                          if(command.get(1).equals("replication") && config.get("--replicaof")==(null)){

                                String print ="role:master";
                                String masterReplId = "master_replid:0123456789abcdef0123456789abcdef01234567";  // 40 chars
                                String master_offset_string="master_repl_offset:0";
                                StringBuilder respKeys = new StringBuilder();
                                respKeys.append("role:master\n")
                                    .append("master_replid:0123456789abcdef0123456789abcdef01234567\n")
                                    .append("master_repl_offset:0\n");
                                String resp1 = "$" + respKeys.length() + "\r\n" + respKeys + "\r\n";
                                outputStream.write(resp1.getBytes());

                          }else{
                                String print ="role:slave";
                                StringBuilder respKeys = new StringBuilder();
                                respKeys.append("$").append(print.length()).append("\r\n").append(print).append("\r\n");
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