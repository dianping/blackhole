package com.dp.blackhole.broker;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

import com.dp.blackhole.common.ParamsKey;
import com.hadoop.compression.lzo.LzopDecompressor;

/**
 * Compression related stuff.
 */
public final class Compression {
    static final Log LOG = LogFactory.getLog(Compression.class);

    /**
     * Prevent the instantiation of class.
     */
    private Compression() {
        // nothing
    }

    static class FinishOnFlushCompressionStream extends FilterOutputStream {
        public FinishOnFlushCompressionStream(CompressionOutputStream cout) {
            super(cout);
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            CompressionOutputStream cout = (CompressionOutputStream) out;
            cout.finish();
            cout.flush();
            cout.resetState();
        }
    }

    /**
     * Compression algorithms.
     */
    public static enum Algorithm {
        LZO(ParamsKey.COMPRESSION_LZO) {
            private static final String defaultClazz = "com.hadoop.compression.lzo.LzopCodec";
            private transient CompressionCodec codec = null;

            @Override
            public synchronized boolean isSupported() {
                if (codec != null) {
                    return true;
                }
                String clazz = defaultClazz;
                try {
                    LOG.info("Trying to load Lzo codec class: " + clazz);
                    codec = (CompressionCodec) ReflectionUtils.newInstance(
                            Class.forName(clazz), conf);
                } catch (ClassNotFoundException e) {
                    // that is okay
                }
                return codec != null;
            }

            @Override
            CompressionCodec getCodec() throws IOException {
                if (!isSupported()) {
                    throw new IOException("LZO codec class not specified.");
                }

                return codec;
            }

            @Override
            public synchronized InputStream createDecompressionStream(
                    InputStream downStream, Decompressor decompressor,
                    int downStreamBufferSize) throws IOException {
                if (!isSupported()) {
                    throw new IOException("LZO codec class not specified.");
                }
                InputStream bis1 = null;
                if (downStreamBufferSize > 0) {
                    bis1 = new BufferedInputStream(downStream,
                            downStreamBufferSize);
                } else {
                    bis1 = downStream;
                }
                conf.setInt("io.compression.codec.lzo.buffersize", 64 * 1024);
                CompressionInputStream cis = codec.createInputStream(bis1,
                        decompressor);
                BufferedInputStream bis2 = new BufferedInputStream(cis,
                        DATA_IBUF_SIZE);
                return bis2;
            }

            @Override
            public synchronized OutputStream createCompressionStream(
                    OutputStream downStream, Compressor compressor,
                    int downStreamBufferSize) throws IOException {
                if (!isSupported()) {
                    throw new IOException("LZO codec class not specified.");
                }
                OutputStream bos1 = null;
                if (downStreamBufferSize > 0) {
                    bos1 = new BufferedOutputStream(downStream,
                            downStreamBufferSize);
                } else {
                    bos1 = downStream;
                }
                conf.setInt("io.compression.codec.lzo.buffersize", 64 * 1024);
                CompressionOutputStream cos = codec.createOutputStream(bos1,
                        compressor);
                BufferedOutputStream bos2 = new BufferedOutputStream(
                        new FinishOnFlushCompressionStream(cos), DATA_OBUF_SIZE);
                return bos2;
            }

            @Override
            public void createIndex(FileSystem fs, Path lzoFile, Path tmp)
                    throws IOException {
                CompressionCodec codec = getCodec();
                if (null == codec) {
                    throw new IOException("Could not find codec");
                }
                ((Configurable) codec).setConf(conf);
                FSDataInputStream is = null;
                FSDataOutputStream os = null;
                Path outputFile = lzoFile.suffix(ParamsKey.LZO_INDEX_SUFFIX);
                Path tmpOutputFile = lzoFile.suffix(ParamsKey.LZO_TMP_INDEX_SUFFIX);

                // Track whether an exception was thrown or not, so we know to
                // either
                // delete the tmp index file on failure, or rename it to the new
                // index file on success.
                boolean indexingSucceeded = false;
                try {
                    is = fs.open(tmp);
                    os = fs.create(tmpOutputFile);
                    LzopDecompressor decompressor = (LzopDecompressor) codec
                            .createDecompressor();
                    // Solely for reading the header
                    codec.createInputStream(is, decompressor);
                    int numCompressedChecksums = decompressor
                            .getCompressedChecksumsCount();
                    int numDecompressedChecksums = decompressor
                            .getDecompressedChecksumsCount();

                    while (true) {
                        // read and ignore, we just want to get to the next int
                        int uncompressedBlockSize = is.readInt();
                        if (uncompressedBlockSize == 0) {
                            break;
                        } else if (uncompressedBlockSize < 0) {
                            throw new EOFException();
                        }

                        int compressedBlockSize = is.readInt();
                        if (compressedBlockSize <= 0) {
                            throw new IOException(
                                    "Could not read compressed block size");
                        }

                        // See LzopInputStream.getCompressedData
                        boolean isUncompressedBlock = (uncompressedBlockSize == compressedBlockSize);
                        int numChecksumsToSkip = isUncompressedBlock ? numDecompressedChecksums
                                : numDecompressedChecksums
                                        + numCompressedChecksums;
                        long pos = is.getPos();
                        // write the pos of the block start
                        os.writeLong(pos - 8);
                        // seek to the start of the next block, skip any
                        // checksums
                        is.seek(pos + compressedBlockSize
                                + (4 * numChecksumsToSkip));
                    }
                    // If we're here, indexing was successful.
                    indexingSucceeded = true;
                } finally {
                    // Close any open streams.
                    if (is != null) {
                        is.close();
                    }

                    if (os != null) {
                        os.close();
                    }

                    if (!indexingSucceeded) {
                        // If indexing didn't succeed (i.e. an exception was
                        // thrown), clean up after ourselves.
                        fs.delete(tmpOutputFile, false);
                    } else {
                        // Otherwise, rename filename.lzo.index.tmp to
                        // filename.lzo.index.
                        fs.rename(tmpOutputFile, outputFile);
                    }
                }
            }
        },

        GZ(ParamsKey.COMPRESSION_GZ) {
            private transient DefaultCodec codec;

            @Override
            synchronized CompressionCodec getCodec() {
                if (codec == null) {
                    codec = new GzipCodec();
                    codec.setConf(conf);
                }

                return codec;
            }

            @Override
            public synchronized InputStream createDecompressionStream(
                    InputStream downStream, Decompressor decompressor,
                    int downStreamBufferSize) throws IOException {
                // Set the internal buffer size to read from down stream.
                if (downStreamBufferSize > 0) {
                    codec.getConf().setInt("io.file.buffer.size",
                            downStreamBufferSize);
                }
                CompressionInputStream cis = codec.createInputStream(
                        downStream, decompressor);
                BufferedInputStream bis2 = new BufferedInputStream(cis,
                        DATA_IBUF_SIZE);
                return bis2;
            }

            @Override
            public synchronized OutputStream createCompressionStream(
                    OutputStream downStream, Compressor compressor,
                    int downStreamBufferSize) throws IOException {
                OutputStream bos1 = null;
                if (downStreamBufferSize > 0) {
                    bos1 = new BufferedOutputStream(downStream,
                            downStreamBufferSize);
                } else {
                    bos1 = downStream;
                }
                codec.getConf().setInt("io.file.buffer.size", 32 * 1024);
                CompressionOutputStream cos = codec.createOutputStream(bos1,
                        compressor);
                BufferedOutputStream bos2 = new BufferedOutputStream(
                        new FinishOnFlushCompressionStream(cos), DATA_OBUF_SIZE);
                return bos2;
            }

            @Override
            public boolean isSupported() {
                return true;
            }
            
            @Override
            public void createIndex(FileSystem fs, Path lzoFile, Path tmp)
                    throws IOException {
                throw new UnsupportedOperationException("just lzo can create index");
            }
        },

        NONE(ParamsKey.COMPRESSION_NONE) {
            @Override
            CompressionCodec getCodec() {
                return null;
            }

            @Override
            public synchronized InputStream createDecompressionStream(
                    InputStream downStream, Decompressor decompressor,
                    int downStreamBufferSize) throws IOException {
                if (downStreamBufferSize > 0) {
                    return new BufferedInputStream(downStream,
                            downStreamBufferSize);
                }
                return downStream;
            }

            @Override
            public synchronized OutputStream createCompressionStream(
                    OutputStream downStream, Compressor compressor,
                    int downStreamBufferSize) throws IOException {
                if (downStreamBufferSize > 0) {
                    return new BufferedOutputStream(downStream,
                            downStreamBufferSize);
                }

                return downStream;
            }

            @Override
            public boolean isSupported() {
                return true;
            }
            
            @Override
            public void createIndex(FileSystem fs, Path lzoFile, Path tmp)
                    throws IOException {
                throw new UnsupportedOperationException("just lzo can create index");
            }
        };

        // We require that all compression related settings are configured
        // statically in the Configuration object.
        protected static final Configuration conf = new Configuration();
        private final String compressName;
        // data input buffer size to absorb small reads from application.
        private static final int DATA_IBUF_SIZE = 1 * 1024;
        // data output buffer size to absorb small writes from application.
        private static final int DATA_OBUF_SIZE = 4 * 1024;

        Algorithm(String name) {
            this.compressName = name;
        }

        public abstract void createIndex(FileSystem fs, Path lzoFile, Path tmp) throws IOException;

        abstract CompressionCodec getCodec() throws IOException;

        public abstract InputStream createDecompressionStream(
                InputStream downStream, Decompressor decompressor,
                int downStreamBufferSize) throws IOException;

        public abstract OutputStream createCompressionStream(
                OutputStream downStream, Compressor compressor,
                int downStreamBufferSize) throws IOException;

        public abstract boolean isSupported();

        public Compressor getCompressor() throws IOException {
            CompressionCodec codec = getCodec();
            if (codec != null) {
                Compressor compressor = CodecPool.getCompressor(codec);
                if (compressor != null) {
                    if (compressor.finished()) {
                        // Somebody returns the compressor to CodecPool but is
                        // still using
                        // it.
                        LOG.warn("Compressor obtained from CodecPool already finished()");
                    } else {
                        LOG.debug("Got a compressor: " + compressor.hashCode());
                    }
                    /**
                     * Following statement is necessary to get around bugs in
                     * 0.18 where a compressor is referenced after returned back
                     * to the codec pool.
                     */
                    compressor.reset();
                }
                return compressor;
            }
            return null;
        }

        public void returnCompressor(Compressor compressor) {
            if (compressor != null) {
                LOG.debug("Return a compressor: " + compressor.hashCode());
                CodecPool.returnCompressor(compressor);
            }
        }

        public Decompressor getDecompressor() throws IOException {
            CompressionCodec codec = getCodec();
            if (codec != null) {
                Decompressor decompressor = CodecPool.getDecompressor(codec);
                if (decompressor != null) {
                    if (decompressor.finished()) {
                        // Somebody returns the decompressor to CodecPool but is
                        // still using
                        // it.
                        LOG.warn("Deompressor obtained from CodecPool already finished()");
                    } else {
                        LOG.debug("Got a decompressor: "
                                + decompressor.hashCode());
                    }
                    /**
                     * Following statement is necessary to get around bugs in
                     * 0.18 where a decompressor is referenced after returned
                     * back to the codec pool.
                     */
                    decompressor.reset();
                }
                return decompressor;
            }

            return null;
        }

        public void returnDecompressor(Decompressor decompressor) {
            if (decompressor != null) {
                LOG.debug("Returned a decompressor: " + decompressor.hashCode());
                CodecPool.returnDecompressor(decompressor);
            }
        }

        public String getName() {
            return compressName;
        }
    }

    public static Algorithm getCompressionAlgorithmByName(String compressName) {
        Algorithm[] algos = Algorithm.class.getEnumConstants();

        for (Algorithm a : algos) {
            if (a.getName().equalsIgnoreCase(compressName)) {
                return a;
            }
        }

        throw new IllegalArgumentException(
                "Unsupported compression algorithm name: " + compressName);
    }

    public static String[] getSupportedAlgorithms() {
        Algorithm[] algos = Algorithm.class.getEnumConstants();

        ArrayList<String> ret = new ArrayList<String>();
        for (Algorithm a : algos) {
            if (a.isSupported()) {
                ret.add(a.getName());
            }
        }
        return ret.toArray(new String[ret.size()]);
    }
}
