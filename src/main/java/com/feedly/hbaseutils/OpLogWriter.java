package com.feedly.hbaseutils;

import static java.lang.String.format;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class OpLogWriter extends Thread {
    
    private final SimpleDateFormat _dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    private final BlockingQueue<String> _messages = new LinkedBlockingQueue<>();
    private final String _path;
    private volatile boolean _stop;
    
    public OpLogWriter(String path) throws IOException {
        _path = path;
        File f = new File(path);
        f.createNewFile();
        if(!f.canWrite()) {
            throw new IllegalArgumentException("can't write op logs, invalid path");
        }
    }
    
    @Override
    public void run() {
        try(BufferedWriter w = new BufferedWriter(new FileWriter(_path))) {
            while(!_stop || !_messages.isEmpty()) {
                String line = _messages.poll(1, TimeUnit.SECONDS);
                if(line != null) {
                    w.write(line);
                    w.write("\n");
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public void requestStop() {
        _stop = true;
    }
    
    public void log(int size, long duration) {
        _messages.add(format("%s %d %d", _dateFormat.format(new Date()), size, duration));
    }
}