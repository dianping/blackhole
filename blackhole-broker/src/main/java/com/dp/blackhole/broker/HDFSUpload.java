package com.dp.blackhole.broker;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.Compressor;

import com.dp.blackhole.broker.Compression.Algorithm;
import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.broker.storage.StorageManager;
import com.dp.blackhole.broker.storage.RollPartition;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.FileMessageSet;
import com.dp.blackhole.storage.MessageAndOffset;

public class HDFSUpload implements Runnable{
    private static final Log LOG = LogFactory.getLog(HDFSUpload.class);
    private static final String TMP_SUFFIX = ".tmp";
    private RollManager mgr;
    private StorageManager manager;
    private FileSystem fs;
    private RollIdent ident;
    private RollPartition roll;
    private boolean uploadSuccess;
    private ByteBuffer newline;
    private String compression;
    private final int BufferSize = 4 * 1024 * 1024;
    
    public HDFSUpload(RollManager mgr, StorageManager manager, FileSystem fs, RollIdent ident, RollPartition roll, String compression) {
        this.mgr = mgr;
        this.manager = manager;
        this.fs = fs;
        this.ident = ident;
        this.roll = roll;
        this.uploadSuccess = false;
        this.compression = compression;
        newline = ByteBuffer.wrap("\n".getBytes(Charset.forName("UTF-8" )));
    }
    
    @Override
    public void run() {
        uploadRoll();
        mgr.reportUpload(ident, compression, uploadSuccess);
    }

    private void uploadRoll() {
        WritableByteChannel outChannel = null;
        Algorithm compressionAlgo = null;
        try {
            compressionAlgo = Compression.getCompressionAlgorithmByName(this.compression);
        } catch (IllegalArgumentException e) {
            compressionAlgo = Compression.getCompressionAlgorithmByName(ParamsKey.COMPRESSION_GZ);
        }
        try {
            String dfsPath = mgr.getRollHdfsPath(ident, compressionAlgo.getName());
            Path tmp = new Path(dfsPath + TMP_SUFFIX);
            FSDataOutputStream fsDataOutputStream = fs.create(tmp);
            Compressor compressor = compressionAlgo.getCompressor();
            OutputStream out = compressionAlgo
                    .createCompressionStream(fsDataOutputStream, compressor, 0);
            outChannel =  Channels.newChannel(out);
            
            Partition p = manager.getPartition(ident.topic, ident.source, false);
            ByteBuffer buffer = ByteBuffer.allocate(BufferSize);
            ByteBufferChannel channel = new ByteBufferChannel(buffer);
            
            long start = roll.startOffset;
            long end = roll.startOffset + roll.length;
            LOG.debug("Uploading " + ident + " in partition: " + p + " [" + start + "~" + end + "]");
            while (start < end) {
                long size = end -start;
                int limit = (int) ((size > BufferSize) ?  BufferSize : size);
                
                FileMessageSet fms = p.read(start, limit);
                if (fms == null) {
                    throw new IOException("can't get FileMessageSet from partition " + p + " with "
                            + Util.toTupleString(start, end, limit) + " when Uploading " + ident);
                }
                fetchFileMessageSet(channel, fms);
                
                buffer.flip();
                ByteBufferMessageSet bms = new ByteBufferMessageSet(buffer, start);
                long realRead = bms.getValidSize();
                
                Iterator<MessageAndOffset> iter = bms.getItertor();
                while (iter.hasNext()) {
                    MessageAndOffset mo = iter.next();
                    outChannel.write(mo.message.payload());
                    outChannel.write(newline);
                    newline.clear();
                }
                buffer.clear();
                start += realRead;
            }
            outChannel.close();
                
            Path dst = new Path(dfsPath);
            if (!HDFSUtil.retryRename(fs, tmp, dst)) {
                throw new IOException("Faild to rename tmp to " + dst);
            }

            uploadSuccess = true;
        } catch (IOException e) {
            LOG.error("IOE cached: ", e);
        } finally {
            try {
                if (outChannel != null) {
                    outChannel.close();
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
