
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PushbackInputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class ReplicaAckWaiter {

    // Example global list
    public static List<ReplicaConnection> replica_copiedList = new ArrayList<>();

    // Call this when processing WAIT <numReplicas> <timeoutMs>
    public static int waitForAcks(int requiredAcks, int timeoutMs, long masterOffset) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;

        replica_copiedList.clear();                 // optional: clear old data
        replica_copiedList.addAll(Main.replicaConnections);   
        System.out.println(replica_copiedList);

        // Step 1: Send REPLCONF GETACK * to all replicas
        for (ReplicaConnection replica : replica_copiedList) {
            try {
                Writer writer = new OutputStreamWriter(replica.getOutputStream(), "UTF-8");
                writer.write("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
                writer.flush();
            } catch (Exception e) {
                // Ignore write failure
            }
        }

        List<ReplicaConnection> acked = new ArrayList<>();

        while (System.currentTimeMillis() < deadline) {
            for (ReplicaConnection replica : replica_copiedList) {
                if (acked.contains(replica)) continue;
                System.out.println("inside");

                InputStream in= replica.getInputStream();
                replica.getSocket().setSoTimeout(10);
                Main.ParseResult result = RESPParser.parseRESP(in);
                
                List<String> command = result.command;
                String cmd = command.get(0).toUpperCase();
                if(cmd.equalsIgnoreCase("REPLCONF") && command.get(1).equalsIgnoreCase("ACK")){
                    replica.setack(true);
                }
                

                if (replica.getOffset() >= masterOffset && replica.getack()) {
                    acked.add(replica);
                }
            }

            if (acked.size() >= requiredAcks) break;
        }

        //     try {
        //         Thread.sleep(10);  // allow time for ACKs to arrive
        //     } catch (InterruptedException ignored) {}
        // }

        //  for (ReplicaConnection replica : replica_copiedList) {
        //     try {
        //         System.out.println(replica);
        //         System.out.println(replica.getOffset());
        //     } catch (Exception e) {
        //         // Ignore write failure
        //     }
        // }

        return acked.size();
    }
}
