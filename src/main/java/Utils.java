import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.Socket;
import java.util.Base64;
import java.util.List;

public class Utils {

    public static void handleClient(Socket clientSocket) {
        try (Socket socket = clientSocket;
             InputStream inputStream = socket.getInputStream();
             OutputStream outputStream = socket.getOutputStream()) {

            while (true) {

                Main.ParseResult result = RESPParser.parseRESP(inputStream);
                List<String> command = result.command;
                if (command.isEmpty()) continue;

                System.out.println("Parsed RESP command: " + command);
                String cmd = command.get(0).toUpperCase();


                 switch (cmd) {

                    case "WAIT":          
                            outputStream.write(":0\r\n".getBytes());
                            outputStream.flush();
                            break;
                            
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

                        Main.map.put(key, new Main.ValueWithExpiry(value, expiryTime));
                        outputStream.write("+OK\r\n".getBytes());
                        outputStream.flush();


                       for (OutputStream replicaOutputStream : Main.replicaConnections) {
                        replicaOutputStream.write(buildRespArray("SET", key, value).getBytes());
                        replicaOutputStream.flush();
                    }                

                        break;

                    case "GET":
                        String getKey = command.get(1);
                        Main.ValueWithExpiry stored = Main.map.get(getKey);

                        if (stored == null
                                || (stored.expiryTimeMillis != 0
                                        && System.currentTimeMillis() > stored.expiryTimeMillis)) {
                            Main.map.remove(getKey);
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
                            String value_1 = Main.config.get(key_1);
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
                                if(!command.get(1).equals("GETACK")){
                                        outputStream.write("+OK\r\n".getBytes());
                                        break;
                                }

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

                                    outputStream.write(rdbBytes);
                                    Main.replicaConnections.add(outputStream); // tcp connections --> so that later master can update the data on replica side.
                        

                                    break;

                    case "KEYS":
                        if (command.get(1).equals("*")) {
                            StringBuilder respKeys = new StringBuilder();
                            respKeys.append("*").append(Main.map.size()).append("\r\n");
                            for (String key_2 : Main.map.keySet()) {
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
                          if(command.get(1).equals("replication") && Main.config.get("--replicaof")==(null)){

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
            sb.append("$").append(arg.length()).append("\r\n");
            sb.append(arg).append("\r\n");
        }
        return sb.toString();
    }
}
