import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ReplicaConnection {
    private final Socket socket;
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private long offset;
    public boolean ack=false;

    public ReplicaConnection(Socket socket) throws Exception {
        this.socket = socket;
        this.inputStream = socket.getInputStream();
        this.outputStream = socket.getOutputStream();
        this.offset = 0;
    }

    public ReplicaConnection(Socket socket, InputStream inputStream, OutputStream outputStream) {
        this.socket = socket;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.offset = 0;
    }

    public Socket getSocket() {
        return socket;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public OutputStream getOutputStream() {    
        return outputStream;
    }

    public long getOffset() {
        return offset;
    }

    public boolean getack(){
        return ack;
    }

    public void setack(boolean ack_or_not){
        this.ack =ack_or_not;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
