import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PushbackInputStream;
import java.io.Writer;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

public class ReplicaAckWaiter {

    //public static List<ReplicaConnection> replica_copiedList = new ArrayList<>();

    public static int waitForAcks(int requiredAcks, int timeoutMs, long masterOffset) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;

        // replica_copiedList.clear();
        // replica_copiedList.addAll(Main.replicaConnections);
        System.out.println("[ReplicaAckWaiter] Replicas: " +  Main.replicaConnections);

        // Step 1: Send REPLCONF GETACK * to all replicas
        for (ReplicaConnection replica : Main.replicaConnections) {
            try {
                Writer writer = new OutputStreamWriter(replica.getOutputStream(), "UTF-8");
                writer.write("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
                writer.flush();
            } catch (Exception e) {
                // Ignore failed write
            }
        }

        List<ReplicaConnection> acked = new ArrayList<>();

        while (System.currentTimeMillis() < deadline) {
            for (ReplicaConnection replica : Main.replicaConnections) {
                if (acked.contains(replica)) continue;

              //  try {
                  //  System.out.println("inside");
                //     InputStream in = replica.getInputStream();
                //    // System.out.println(in);
                //     if (in.available() > 0) {
                //          PushbackInputStream pin = new PushbackInputStream(in);
                //          Utils.skipUntilStar(pin);  // Skips preamble if needed

                //         Main.ParseResult result2 = RESPParser.parseRESP(pin);
                //         List<String> command = result2.command;
                //        //  System.out.println("inside");

                //         if (command.size() >= 3 &&
                //             command.get(0).equalsIgnoreCase("REPLCONF") &&
                //             command.get(1).equalsIgnoreCase("ACK")) {

                //             long replicaOffset = Long.parseLong(command.get(2));
                //             replica.setOffset(replicaOffset);
                //             replica.setack(true);
                //         }

                        if (replica.getOffset() >= masterOffset && replica.getack()) {
                            acked.add(replica);
                            System.out.println("[ReplicaAckWaiter] Replica acknowledged: " + replica);
                        }

               //  catch (SocketTimeoutException ste) {
                    // Normal case, just skip
                // catch (IOException | NumberFormatException | IndexOutOfBoundsException e) {
                //     // Skip malformed or broken replicas
                // }
            }
            if (acked.size() >= requiredAcks) break;
        }

        //     try {
        //         Thread.sleep(5); // prevent CPU spinning
        //     } catch (InterruptedException ignored) {}
        // }

        return acked.size();
    }
}

 