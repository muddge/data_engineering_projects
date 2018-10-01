package com.muddge;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

public class FileExtractorUtil {

    private FileInputStream fileInputStream;
    private final String extractedFile;

    public FileExtractorUtil(String zippedFile) {
        this.extractedFile = zippedFile.substring(0, zippedFile.lastIndexOf('.'));
        try {
            this.fileInputStream = new FileInputStream(zippedFile);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(FileExtractorUtil.class.getName()).log(Level.SEVERE, null, ex);
            throw new IllegalArgumentException(ex.getMessage());
        }
    }

    /**
     * Use GunZip
     */
    public void extract() {
        byte[] buffer = new byte[1024];
        try (GZIPInputStream gzis = new GZIPInputStream(this.fileInputStream);
                FileOutputStream outStream = new FileOutputStream(this.getExtractedFile());) {
            int len;
            while ((len = gzis.read(buffer)) > 0) {
                outStream.write(buffer, 0, len);
            }
        } catch (IOException ex) {
            Logger.getLogger(FileExtractorUtil.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage());
        }
    }

    /**
     * @return the extractedFile
     */
    public String getExtractedFile() {
        return extractedFile;
    }

    // public static void main(String[] args) {
    //     new FileExtractorUtil("/home/cloudera/Downloads/2018-01-01-15.json.gz").extract();
    // }

}
