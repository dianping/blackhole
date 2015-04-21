package com.dp.blackhole.broker.ftp;

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
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.io.compress.Compressor;

import com.dp.blackhole.broker.ByteBufferChannel;
import com.dp.blackhole.broker.Compression;
import com.dp.blackhole.broker.RollIdent;
import com.dp.blackhole.broker.RollManager;
import com.dp.blackhole.broker.Compression.Algorithm;
import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.broker.storage.RollPartition;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
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
        this.newline = ByteBuffer.wrap("\n".getBytes(Charset.forName("UTF-8")));
    }
    
    @Override
    public void run() {
        FTPClient ftp = new FTPClient();
        String remoteTempFilename = null;
        OutputStream out = null;
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
            ftp.setFileTransferMode(FTPClient.BINARY_FILE_TYPE);
            ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
            if(!ftp.changeWorkingDirectory("/")) {
                throw new IOException("Can not change to root dir /");
            }
            String remoteDir = mgr.getParentPath(configration.getRootDir(), ident);
            ftpCreateDirectoryTree(ftp, remoteDir);
            LOG.debug("Current FTP working directory is: " + ftp.printWorkingDirectory());
            String remoteFilename = mgr.getGZCompressedFileName(ident);
            remoteTempFilename = "." + remoteFilename + ".tmp";
            Algorithm compressionAlgo = Compression.getCompressionAlgorithmByName(ParamsKey.COMPRESSION_GZ);
            Compressor compressor = compressionAlgo.getCompressor();
            out = compressionAlgo.createCompressionStream(
                    ftp.storeFileStream(remoteTempFilename), compressor, 0);
            remoteWrite(out);
            
            if (!ftp.completePendingCommand()) {
                throw new IOException("Unfinished upload " + remoteFilename);
            }
            if(!ftp.rename(remoteTempFilename, remoteFilename)) {
                throw new IOException("Unfinished rename to " + remoteFilename);
            }
            LOG.info(remoteFilename + " uploaded.");
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
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                }
            }
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException e) {
                }
            }
        }
    }
    
    private void remoteWrite(OutputStream out) throws IOException {
        WritableByteChannel compressionFtpChannel =  Channels.newChannel(out);
        ByteBuffer buffer = ByteBuffer.allocate(BufferSize);
        ByteBufferChannel channel = new ByteBufferChannel(buffer);
        
        long start = roll.startOffset;
        long end = roll.startOffset + roll.length;
        LOG.debug("FTP Uploading " + ident + " in partition: " + partition + " [" + start + "~" + end + "]");
        while (start < end) {
            long size = end -start;
            int limit = (int) ((size > BufferSize) ?  BufferSize : size);
            
            FileMessageSet fms = partition.read(start, limit);
            if (fms == null) {
                throw new IOException("can't get FileMessageSet from partition " + partition + " with "
                        + Util.toTupleString(start, end, limit) + " when Uploading " + ident);
            }
            fetchFileMessageSet(channel, fms);
            
            buffer.flip();
            ByteBufferMessageSet bms = new ByteBufferMessageSet(buffer, start);
            long realRead = bms.getValidSize();
            
            Iterator<MessageAndOffset> iter = bms.getItertor();
            while (iter.hasNext()) {
                MessageAndOffset mo = iter.next();
                compressionFtpChannel.write(mo.getMessage().payload());
                compressionFtpChannel.write(newline);
                newline.clear();
            }
            buffer.clear();
            start += realRead;
        }
        compressionFtpChannel.close();
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

    /**
     * utility to create an arbitrary directory hierarchy on the remote ftp
     * server
     * 
     * @param client
     * @param dirTree
     *            the directory tree only delimited with / chars. No file name!
     * @throws Exception
     */
    private void ftpCreateDirectoryTree(FTPClient client, String dirTree) throws IOException {
        boolean dirExists = true;
        // tokenize the string and attempt to change into each directory level.
        // If you cannot, then start creating.
        String[] directories = dirTree.split("/");
        for (String dir : directories) {
            if (!dir.isEmpty()) {
                if (dirExists) {
                    dirExists = client.changeWorkingDirectory(dir);
                }
                if (!dirExists) {
                    if (!client.makeDirectory(dir)) {
                        throw new IOException(
                                "Unable to create remote directory '" + dir
                                        + "'.  error='"
                                        + client.getReplyString() + "'");
                    }
                    if (!client.changeWorkingDirectory(dir)) {
                        throw new IOException(
                                "Unable to change into newly created remote directory '"
                                        + dir + "'.  error='"
                                        + client.getReplyString() + "'");
                    }
                }
            }
        }
    }
}
