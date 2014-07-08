package com.dp.blackhole.broker.ftp;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import com.dp.blackhole.broker.ByteBufferChannel;
import com.dp.blackhole.broker.RollIdent;
import com.dp.blackhole.broker.RollManager;
import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.broker.storage.RollPartition;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.FileMessageSet;
import com.dp.blackhole.storage.MessageAndOffset;

public class FTPUpload implements Runnable {
    private static final Log LOG = LogFactory.getLog(FTPUpload.class);
    private RollManager mgr;
    private FTPConfigration configration;
    private RollIdent ident;
    private RollPartition roll;
    private Partition partition;
    private int BufferSize = 4 * 1024 * 1024;
    private ByteBuffer newline;
    
    public FTPUpload(RollManager mgr, FTPConfigration configration, RollIdent ident, RollPartition roll, Partition partition) {
        this.mgr = mgr;
        this.configration = configration;
        this.ident = ident;
        this.roll = roll;
        this.partition = partition;
        this.newline = ByteBuffer.wrap("\n".getBytes(Charset.forName("UTF-8" )));
    }
    
    @Override
    public void run() {
        FTPClient ftp = new FTPClient();
        String remoteTempFilename = null;
        try {
            int reply;
            ftp.connect(configration.getUrl(), configration.getPort());
            ftp.login(configration.getUsername(), configration.getPassword());
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                LOG.error("FTPReply is " + reply + ", disconnected.");
                return;
            }
            ftp.changeWorkingDirectory(configration.getRootDir());
            String remoteFilename = mgr.getRollFtpPath(configration.getRootDir(), ident);
            remoteTempFilename = remoteFilename + ".tmp";
            OutputStream out = new GZIPOutputStream(ftp.appendFileStream(remoteTempFilename));
            
            remoteWrite(out);
            
            if (!ftp.completePendingCommand()) {
                throw new IOException("Unfinished upload.");
            }
            if(!ftp.rename(remoteTempFilename, remoteFilename)) {
                throw new IOException("Unfinished rename.");
            }
            
            ftp.logout();
        } catch (IOException e) {
            LOG.error("Oops, got an excepion.", e);
            if (ftp.isConnected() && remoteTempFilename != null) {
                try {
                    ftp.deleteFile(remoteTempFilename);
                } catch (IOException e1) {
                }
            }
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException e2) {
                }
            }
        }
    }
    
    private void remoteWrite(OutputStream out) throws IOException {
        WritableByteChannel gzFtpChannel =  Channels.newChannel(out);
        ByteBuffer buffer = ByteBuffer.allocate(BufferSize);
        ByteBufferChannel channel = new ByteBufferChannel(buffer);
        
        long start = roll.startOffset;
        long end = roll.startOffset + roll.length;
        LOG.debug("FTP Uploading " + ident + " in partition: " + partition + " [" + start + "~" + end + "]");
        while (start < end) {
            long size = end -start;
            int limit = (int) ((size > BufferSize) ?  BufferSize : size);
            
            FileMessageSet fms = partition.read(start, limit);
            fetchFileMessageSet(channel, fms);
            
            buffer.flip();
            ByteBufferMessageSet bms = new ByteBufferMessageSet(buffer, start);
            long realRead = bms.getValidSize();
            
            Iterator<MessageAndOffset> iter = bms.getItertor();
            while (iter.hasNext()) {
                MessageAndOffset mo = iter.next();
                gzFtpChannel.write(mo.message.payload());
                gzFtpChannel.write(newline);
                newline.clear();
            }
            buffer.clear();
            start += realRead;
        }
        gzFtpChannel.close();
    }
    
    private void fetchFileMessageSet(GatheringByteChannel channel, FileMessageSet messages) throws IOException {
        int read = 0;
        int limit = messages.getSize();
        while (read < limit) {
            read += fetchChunk(channel, messages, read, limit - read);
        }
    }
    
    private int fetchChunk(GatheringByteChannel channel, FileMessageSet messages, int start, int limit) throws IOException {
        int read = 0;
        while (read < limit) {
            read += messages.write(channel, start + read, limit - read);
        }
        return read;
    }
}
