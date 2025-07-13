import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class RESPParser {

    public  static Main.ParseResult parseRESP(InputStream in) throws IOException {
        System.out.println("entered parseresp");
        List<String> result = new ArrayList<>();
        int bytesRead = 0;
        DataInputStream reader = new DataInputStream(in);

        int b = reader.read();
        bytesRead += 1;
        if ((char) b != '*') {
            throw new IOException("Expected RESP array (starts with '*')");
        }

        String numArgsLine = readLine(reader);
        bytesRead += numArgsLine.length() + 2;
        int numArgs = Integer.parseInt(numArgsLine);

        for (int i = 0; i < numArgs; i++) {
            char prefix = (char) reader.read();
            bytesRead += 1;
            if (prefix != '$') {
                throw new IOException("Expected bulk string (starts with '$')");
            }

            String lenLine = readLine(reader);
            bytesRead += lenLine.length() + 2;
            int length = Integer.parseInt(lenLine);

            byte[] buf = new byte[length];
            reader.readFully(buf);
            bytesRead += length;
            result.add(new String(buf));

            readLine(reader);
            bytesRead += 2;
        }

        //  System.out.println(result);
        //  System.out.println(bytesRead);

          return new Main.ParseResult(result, bytesRead);
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