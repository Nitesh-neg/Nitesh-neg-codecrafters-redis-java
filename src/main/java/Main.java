import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class  Main {
    static class Entry {
        final String value;
        final long expiryMillis; // -1 if no expiry

        Entry(String value, long expiryMillis) {
            this.value = value;
            this.expiryMillis = expiryMillis;
        }
    }

    private final Map<String, Entry> db = new LinkedHashMap<>();
    
    public static void main(String[] args) throws Exception {
        String dir = ".";
        String dbfilename = "dump.rdb";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--dir":
                    if (i + 1 < args.length) dir = args[++i];
                    break;
                case "--dbfilename":
                    if (i + 1 < args.length) dbfilename = args[++i];
                    break;
            }
        }
        Main server = new Main();
        File rdb = new File(dir, dbfilename);
        if (rdb.exists()) {
            try (InputStream in = new FileInputStream(rdb)) {
                server.loadRdb(in);
            }
        }
        server.start();
    }

    private void start() throws IOException {
        ServerSocket ss = new ServerSocket(6379);
        while (true) {
            try (Socket client = ss.accept()) {
                handle(client);
            }
        }
    }

    private void handle(Socket client) throws IOException {
        BufferedInputStream in = new BufferedInputStream(client.getInputStream());
        BufferedOutputStream out = new BufferedOutputStream(client.getOutputStream());
        List<String> command = readCommand(in);
        if (command.size() >= 1 && "KEYS".equalsIgnoreCase(command.get(0))) {
            // Only handle KEYS *
            sendArray(out, new ArrayList<>(db.keySet()));
        } else {
            String err = "-ERR unknown command\r\n";
            out.write(err.getBytes(StandardCharsets.UTF_8));
            out.flush();
        }
    }

    private List<String> readCommand(BufferedInputStream in) throws IOException {
        String line = readLine(in);
        if (line.isEmpty() || line.charAt(0) != '*') return Collections.emptyList();
        int count = Integer.parseInt(line.substring(1));
        List<String> res = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String lenLine = readLine(in); // $<len>
            if (lenLine.isEmpty() || lenLine.charAt(0) != '$') return Collections.emptyList();
            int len = Integer.parseInt(lenLine.substring(1));
            byte[] buf = in.readNBytes(len);
            in.read(); // \r
            in.read(); // \n
            res.add(new String(buf, StandardCharsets.UTF_8));
        }
        return res;
    }

    private String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read(); // expecting \n
                break;
            }
            bos.write(b);
        }
        return bos.toString(StandardCharsets.UTF_8);
    }

    private void sendArray(OutputStream out, List<String> data) throws IOException {
        out.write(('*' + String.valueOf(data.size()) + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (String item : data) {
            byte[] bytes = item.getBytes(StandardCharsets.UTF_8);
            out.write(('$' + String.valueOf(bytes.length) + "\r\n").getBytes(StandardCharsets.UTF_8));
            out.write(bytes);
            out.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        out.flush();
    }

    private int readByte(InputStream in) throws IOException {
        int b = in.read();
        if (b == -1) throw new EOFException();
        return b & 0xFF;
    }

    private long readLE(InputStream in, int len) throws IOException {
        long res = 0;
        for (int i = 0; i < len; i++) {
            res |= ((long) readByte(in)) << (8 * i);
        }
        return res;
    }

    private long readBE(InputStream in, int len) throws IOException {
        long res = 0;
        for (int i = 0; i < len; i++) {
            res = (res << 8) | readByte(in);
        }
        return res;
    }

    private long decodeSize(InputStream in, int first) throws IOException {
        int type = (first & 0xC0) >> 6;
        if (type == 0) {
            return first & 0x3F;
        } else if (type == 1) {
            return ((first & 0x3F) << 8) | readByte(in);
        } else if (type == 2) {
            return readBE(in, 4);
        } else {
            throw new IOException("Invalid size encoding");
        }
    }

    private String decodeString(InputStream in) throws IOException {
        int first = readByte(in);
        int type = (first & 0xC0) >> 6;
        if (type != 3) {
            long len = decodeSize(in, first);
            byte[] buf = in.readNBytes((int) len);
            return new String(buf, StandardCharsets.UTF_8);
        } else {
            int enc = first & 0x3F;
            if (enc == 0) {
                int val = readByte(in);
                return Integer.toString(val);
            } else if (enc == 1) {
                long val = readLE(in, 2);
                return Long.toString(val);
            } else if (enc == 2) {
                long val = readLE(in, 4);
                return Long.toString(val);
            } else {
                throw new IOException("Unsupported string encoding");
            }
        }
    }

    private void loadRdb(InputStream in) throws IOException {
        byte[] header = in.readNBytes(9);
        String magic = new String(header, StandardCharsets.UTF_8);
        if (!magic.startsWith("REDIS")) {
            throw new IOException("Invalid RDB file");
        }
        long expiry = -1;
        while (true) {
            int b = in.read();
            if (b == -1) break;
            if (b == 0xFF) {
                break; // EOF
            } else if (b == 0xFE) { // DB selector
                int first = readByte(in);
                decodeSize(in, first); // skip db index
            } else if (b == 0xFB) {
                int first1 = readByte(in);
                decodeSize(in, first1); // skip ht size
                int first2 = readByte(in);
                decodeSize(in, first2); // skip expire ht size
            } else if (b == 0xFA) {
                decodeString(in); // metadata name
                decodeString(in); // metadata value
            } else if (b == 0xFD) { // expire seconds
                expiry = readLE(in, 4) * 1000L;
                int valType = readByte(in);
                parseKeyValue(in, valType, expiry);
                expiry = -1;
            } else if (b == 0xFC) { // expire ms
                expiry = readLE(in, 8);
                int valType = readByte(in);
                parseKeyValue(in, valType, expiry);
                expiry = -1;
            } else {
                parseKeyValue(in, b, -1);
            }
        }
    }

    private void parseKeyValue(InputStream in, int valType, long expiry) throws IOException {
        if (valType != 0) {
            throw new IOException("Only string values supported");
        }
        String key = decodeString(in);
        String value = decodeString(in);
        db.put(key, new Entry(value, expiry));
    }
}