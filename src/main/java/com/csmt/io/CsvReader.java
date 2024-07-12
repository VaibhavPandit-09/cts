package com.csmt.io;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CsvReader {
    
    //Read csv with appache commons csv
    public static Iterable<CSVRecord> readCsv(String path) throws IOException {

        FileReader filereader = new FileReader(path);
        Iterable<CSVRecord> records = CSVFormat.EXCEL.builder()
                .build()
                .parse(filereader);
        return records;
    }

    public static CSVParser readCsvParse(String path) throws IOException {
        Reader fileReader = new FileReader(path);
        return CSVFormat.EXCEL
                .withFirstRecordAsHeader()
                .parse(fileReader);
    }

    public static String getCsvName(String path) throws IOException {
        File file = new File(path);
        String name = file.getName();
        name = name.substring(0, name.lastIndexOf("."));

        return name;

    }
    
    //get headers in list form

}
