package com.dp.blackhole.collectornode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dp.blackhole.collectornode.persistent.Partition;
import com.dp.blackhole.collectornode.persistent.PersistentManager;
import com.dp.blackhole.collectornode.persistent.RollPartition;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.FileMessageSet;
import com.dp.blackhole.storage.MessageAndOffset;

public class HDFSUpload implements Runnable{
    private static final Log LOG = LogFactory.getLog(HDFSUpload.class);
    private static final String TMP_SUFFIX = ".tmp";
    private RollManager mgr;
    private PersistentManager manager;
    private FileSystem fs;
    private RollIdent ident;
    private RollPartition roll;
    private boolean uploadSuccess;
    private ByteBuffer newline;
    private int BufferSize = 4 * 1024 * 1024;
    
    public HDFSUpload(RollManager mgr, PersistentManager manager, FileSystem fs, RollIdent ident, RollPartition roll) {
        this.mgr = mgr;
        this.manager = manager;
        this.fs = fs;
        this.ident = ident;
        this.roll = roll;
        this.uploadSuccess = false;
        newline = ByteBuffer.wrap("\n".getBytes(Charset.forName("UTF-8" )));
    }
    
    @Override
    public void run() {
        uploadRoll();
        mgr.reportUpload(ident, uploadSuccess);
    }

    private void uploadRoll() {
        WritableByteChannel gzChannel = null;
        try {
            String dfsPath = mgr.getRollHdfsPath(ident);
            Path tmp = new Path(dfsPath + TMP_SUFFIX);
            gzChannel =  Channels.newChannel(new GZIPOutputStream(fs.create(tmp)));
            
            Partition p = manager.getPartition(ident.app, ident.source);
    
            ByteBuffer buffer = ByteBuffer.allocate(BufferSize);
            ByteBufferChannel channel = new ByteBufferChannel(buffer);
            
            long start = roll.startOffset;
            long end = roll.startOffset + roll.length;
    
            while (start < end) {
                int limit = (int) (end -start);
                limit = (limit > BufferSize) ?  BufferSize : limit;
                        
                FileMessageSet fms = p.read(start, limit);
                fetchFileMessageSet(channel, fms);
                
                buffer.flip();
                ByteBufferMessageSet bms = new ByteBufferMessageSet(buffer, start);
                long realRead = bms.getValidSize();
                
                Iterator<MessageAndOffset> iter = bms.getItertor();
                while (iter.hasNext()) {
                    MessageAndOffset mo = iter.next();
                    gzChannel.write(mo.message.payload());
                    gzChannel.write(newline);
                    newline.clear();
                }
                gzChannel.close();
                buffer.clear();
                start += realRead;
            }
                
            Path dst = new Path(dfsPath);
            if (!HDFSUtil.retryRename(fs, tmp, dst)) {
                throw new IOException("Faild to rename tmp to " + dst);
            }

            uploadSuccess = true;
        } catch (IOException e) {
            LOG.error("IOE cached: ", e);
        } finally {
            try {
                if (gzChannel != null) {
                    gzChannel.close();
                }
            } catch (IOException e) {
            }

        }
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
