package com.feedly.hbaseutils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hbase.util.Bytes;

class Message {
    
    private long _seq = -1;
    private byte[] _bytes;
    
    public Message(long seq, byte[] bytes) {
        _seq = seq;
        _bytes = bytes;
    }
    
    public void send(OutputStream out, byte[] buffer) throws IOException {
        int offset = Bytes.putLong(buffer, 0, _seq);
        offset = Bytes.putInt(buffer, offset, _bytes.length);

        int capacity = buffer.length - offset;
        int remaining = _bytes.length;
        
        int dataOffset = 0;
        while(remaining > 0) {
            int toWrite = Math.min(capacity, remaining);
            System.arraycopy(_bytes, dataOffset, buffer, offset, toWrite);
            out.write(buffer, 0, toWrite + offset);
            out.flush();
            offset = 0;
            capacity = buffer.length;
            remaining -= toWrite;
            dataOffset += toWrite;
        }
    }
    
    public static Message receive(InputStream in, byte[] buffer) throws IOException {

        for(int i = 0; i < 12; i++) {
            try {
                int b = in.read();
                if(b < 0) {
                    if(i == 0)
                        return null; //no more messages
                    
                    throw new IOException("underflow: header not read");
                }
                buffer[i] = (byte) b;
            } catch(IOException ex) {
                if(i > 0)
                    throw ex;
                
                return null;
            }
        }
        
        long seq = Bytes.toLong(buffer, 0);
        byte[] bytes = new byte[Bytes.toInt(buffer, 8)];
        
        
        int totalRead = 0;
        while(totalRead < bytes.length) {
            int toRead = Math.min(bytes.length - totalRead, buffer.length);
            int read = in.read(buffer, 0, toRead);
            if(read < 0)
                throw new IOException("underflow: data not read, expected " + bytes.length + " but read " + totalRead);
           
            System.arraycopy(buffer, 0, bytes, totalRead, read);
            totalRead += read;
        }
        
        return new Message(seq, bytes);
    }
    
    long seq() { return _seq; }
    byte[] bytes() { return _bytes; }
}
