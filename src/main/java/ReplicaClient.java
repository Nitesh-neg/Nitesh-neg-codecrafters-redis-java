import java.io.*;
import java.net.Socket;
import java.util.List;

public class ReplicaClient {

    public static void connectToMaster() {
        
        try {
                String[] parts = Main.config.get("--replicaof").split(" ");
                String masterHost = (parts[0]).toString();
                int masterPort = Integer.parseInt(parts[1]);


                Socket socket = new Socket(masterHost, masterPort);
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                byte[] buffer = new byte[10000];

                String pingCommand = "*1\r\n$4\r\nPING\r\n";
                out.write(pingCommand.getBytes());
                out.flush();

                int bytesRead = in.read(buffer);
                // if (bytesRead == -1) {
                //  //   socket.close();
                //     return;
                //     }

                String replconf1Resp =
                        Utils.buildRespArray(
                                "REPLCONF", "listening-port", String.valueOf(Main.config.get("--port")));
                out.write(replconf1Resp.getBytes());
                out.flush();

                bytesRead = in.read(buffer);
                // if (bytesRead == -1) {
                //  //   socket.close();
                //     return;
                //     }

                //String reply = new String(buffer, 0, bytesRead).trim();
                // if (!reply.equals("+OK")) {
                //     //    socket.close();
                //         return;
                //     }

                String replconf2Resp = Utils.buildRespArray("REPLCONF", "capa", "psync2");
                out.write(replconf2Resp.getBytes());
                out.flush();

                bytesRead = in.read(buffer);// in.read(buffer) gives --> number of bytes actually read from stream to buffer
                // if (bytesRead == -1) {     // in.read()--> only read one byte
                // //    socket.close();
                //     return;
                // }

                //reply = new String(buffer, 0, bytesRead).trim();
                //   if (!reply.equals("+OK")) {
                //       //  socket.close();
                //         return;
                //     }

                String psync = Utils.buildRespArray("PSYNC", "?", "-1");
                out.write(psync.getBytes());
                out.flush();

                //  bytesRead = in.read(buffer);
                // if(bytesRead ==-1){
                //  //   socket.close();
                //     return;
                // }

                //reply = new String(buffer, 0, bytesRead).trim();
                // if(!reply.equals("+FULLRESYNC")){
                //     return;
                // }

                PushbackInputStream pin = new PushbackInputStream(in);
                Utils.skipUntilStar(pin);

                while (true) {

                    Main.ParseResult result = RESPParser.parseRESP(pin);
                    List<String> command = result.command;
                    // for(int i=0;i<command.size();i++){
                    //     availableBytes+=(command.get(i).getBytes()).length ;
                    // }
                    if (command.isEmpty()) continue;

                    String cmd = command.get(0).toUpperCase();

                    switch (cmd) {

                        case "PING":

                            Main.offset+=result.bytesConsumed;

                             break;
                                
                        case "SET":

                            String key = command.get(1);
                            String value = command.get(2);
                            long expiryTime = Long.MAX_VALUE;

                            // if (command.size() >= 5 && command.get().equalsIgnoreCase("PX")) {
                            //     try {
                            //         long pxMillis = Long.parseLong(command.get(4));
                            //         expiryTime = System.currentTimeMillis() + pxMillis;
                            //     } catch (NumberFormatException e) {
                            //         out.write("-ERR invalid PX value\r\n".getBytes());
                            //         continue;
                            //     }
                            // }

                            Main.map.put(key, new Main.ValueWithExpiry(value, expiryTime));
                            Main.offset+=result.bytesConsumed;
                            break;

                        case "REPLCONF":
                                
                                String response ="*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n"+"$"+String.valueOf(Main.offset).length()+"\r\n"+Main.offset+"\r\n";
                                Main.offset+=result.bytesConsumed;
                                out.write(response.getBytes());
                                out.flush();
                                break;
                                
                        default:
                            
                            System.out.println("not reciving ");
                            System.out.flush();
                            break;
                    }
                }
            
        }    

                catch (IOException e) {

                    System.out.println("Replica connection error: " + e.getMessage());
                }
        }   
    }