package com.zhp.fileutils.csv;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import org.apache.commons.io.IOUtils;

public class CSVFileWriter implements Closeable {
    private CSVWorker csvBuffer = new CSVWorker();
    private FileOutputStream fos = null;
    private PrintWriter writer = null;
    public static final byte[] BOM = new byte[]{(byte) 0xef, (byte) 0xbb, (byte) 0xbf};

    public CSVFileWriter(String filePath) {
        this(filePath, false);
    }

    public CSVFileWriter(String filePath, boolean append) {
        this(new File(filePath), append);
    }

    public CSVFileWriter(File file, boolean append) {
        try {
            file.getParentFile().mkdirs();
            this.fos = new FileOutputStream(file, append);
            if (!append) {
                fos.write(BOM);
            }
            this.writer = new PrintWriter(new OutputStreamWriter(fos, "UTF-8"));
            this.csvBuffer.clear();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeToBuffer(Object o) {
        this.csvBuffer.put(o.toString());
    }

    public void doWrite() {
        this.writer.println(this.csvBuffer.toString());
        this.csvBuffer.clear();
    }

    public void clearBuffer() {
        this.csvBuffer.clear();
    }

    public void writeLine(String line) {
        this.writer.println(line);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.writer);
        IOUtils.closeQuietly(this.fos);
    }
}
