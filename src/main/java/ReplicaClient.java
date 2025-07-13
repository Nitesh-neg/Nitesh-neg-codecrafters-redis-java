import java.io.*;
import java.net.Socket;
import java.util.List;

public class ReplicaClient {

    public static void connectToMaster() {

        try {
            String[] parts = Main.config.get("replicaof").split(" ");
            String masterHost = parts[0];
            int masterPort = Integer.parseInt(parts[1]);

            Socket socket = new Socket(masterHost, masterPort);
            ReplicaConnection connection = new ReplicaConnection(socket);

            OutputStream out = connection.getOutputStream();
            InputStream in = connection.getInputStream();
            byte[] buffer = new byte[10000];

            // PING
            String pingCommand = "*1\r\n$4\r\nPING\r\n";
            out.write(pingCommand.getBytes());
            out.flush();
            in.read(buffer);  // Read PONG reply

            // REPLCONF 1
            String replconf1Resp = Utils.buildRespArray("REPLCONF", "listening-port", String.valueOf(Main.config.get("port")));
            out.write(replconf1Resp.getBytes());
            out.flush();
            in.read(buffer);  // Read +OK

            // REPLCONF 2
            String replconf2Resp = Utils.buildRespArray("REPLCONF", "capa", "psync2");
            out.write(replconf2Resp.getBytes());
            out.flush();
            in.read(buffer);  // Read +OK

            // PSYNC
            String psync = Utils.buildRespArray("PSYNC", "?", "-1");
            out.write(psync.getBytes());
            out.flush();
            // in.read(buffer);  // Read +FULLRESYNC

            System.out.println("*****************************************************");

            PushbackInputStream pin = new PushbackInputStream(in);
            Utils.skipUntilStar(pin);  // Skips preamble if needed

            // Main replication loop
            while (true) {

                System.out.println("entered replicaCLient *********************************************************************************");

                Main.ParseResult result = RESPParser.parseRESP(pin);
                System.out.println("entered replicaCLient");
                List<String> command = result.command;
                int offset=result.bytesConsumed;
                System.out.println(offset);

                if (command.isEmpty()) continue;

                String cmd = command.get(0).toUpperCase();

                switch (cmd) {
                    case "PING":
                        connection.setOffset(connection.getOffset() + result.bytesConsumed);
                        break;

                    // case "SET":
                    //     String key = command.get(1);
                    //     String value = command.get(2);
                    //     long expiryTime = Long.MAX_VALUE;
                    //     Main.map.put(key, new Main.ValueWithExpiry(value, expiryTime));
                    //     connection.setOffset(connection.getOffset() + result.bytesConsumed);
                    //     System.out.println("**********************************************************************8");
                    //     break;

                    case "REPLCONF":
                        String response = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n" +
                                "$" + String.valueOf(connection.getOffset()).length() + "\r\n" + connection.getOffset() + "\r\n";
                              //  System.out.println("recieved bytes" + connection);
                                connection.setOffset(connection.getOffset() + result.bytesConsumed);
                              //  System.out.println(connection.getOffset());
                              //  System.out.flush();
                                out.write(response.getBytes());
                                out.flush();
                      
                        break;

                    default:
                        System.out.println("Unknown command: " + cmd);
                        System.out.flush();
                        break;
                }
            }

        } catch (IOException e) {
            System.out.println("Replica connection error: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
        }
    }
}
