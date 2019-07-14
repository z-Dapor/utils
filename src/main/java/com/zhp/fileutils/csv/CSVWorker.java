package com.zhp.fileutils.csv;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class CSVWorker {
    private StringWriter sw = new StringWriter();
    private CSVWriter csvWriter = new CSVWriter(sw);
    private List<String> line = new ArrayList<String>();

    public CSVWorker() {
        this.clear();
    }

    public CSVWorker put(String value) {
        line.add(value);
        return this;
    }

    public void clear() {
        line.clear();
    }

    @Override
    public String toString() {
        sw.getBuffer().setLength(0);
        csvWriter.writeNext(line.toArray(new String[line.size()]));
        StringBuffer sb = sw.getBuffer();
        sb.deleteCharAt(sb.length() - 1);
        return sw.toString();
    }
}
