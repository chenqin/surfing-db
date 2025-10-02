package com.pinterest.drsquirrel.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utility class for reading Parquet files from a directory.
 */
public class ParquetFolderReader {

    /**
     * Read all Parquet files from a directory into a list of VectorSchemaRoots.
     *
     * @param folderPath The directory containing Parquet files
     * @param allocator  The Arrow buffer allocator
     * @return List of VectorSchemaRoot, one per Parquet file
     * @throws IOException If reading fails
     */
    public static List<VectorSchemaRoot> readFolder(String folderPath, BufferAllocator allocator) throws IOException {
        List<VectorSchemaRoot> roots = new ArrayList<>();
        File folder = new File(folderPath);

        if (!folder.exists() || !folder.isDirectory()) {
            throw new IOException("Path does not exist or is not a directory: " + folderPath);
        }

        File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".parquet"));
        if (files == null || files.length == 0) {
            return roots;
        }

        // Sort files for deterministic ordering
        Arrays.sort(files);

        Configuration conf = new Configuration();
        for (File file : files) {
            VectorSchemaRoot root = readParquetFile(file.getAbsolutePath(), allocator, conf);
            if (root != null) {
                roots.add(root);
            }
        }

        return roots;
    }

    /**
     * Read a single Parquet file into a VectorSchemaRoot.
     *
     * @param filePath  The path to the Parquet file
     * @param allocator The Arrow buffer allocator
     * @param conf      Hadoop configuration
     * @return VectorSchemaRoot containing the data
     * @throws IOException If reading fails
     */
    public static VectorSchemaRoot readParquetFile(String filePath, BufferAllocator allocator, Configuration conf) throws IOException {
        Path path = new Path(filePath);
        HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);

        // Use JNI implementation for better compatibility
        try {
            return com.pinterest.drsquirrel.jni.NativeParquetIO.readParquetFolder(allocator, new File(filePath).getParent());
        } catch (UnsatisfiedLinkError e) {
            throw new IOException("Native Parquet reader not available. Please ensure libsurfingthriftjni.so is in java.library.path", e);
        } catch (Exception e) {
            throw new IOException("Failed to read Parquet file: " + filePath, e);
        }
    }

    /**
     * Count total rows across all Parquet files in a folder.
     *
     * @param folderPath The directory containing Parquet files
     * @return Total row count
     * @throws IOException If reading fails
     */
    public static long countRows(String folderPath) throws IOException {
        File folder = new File(folderPath);

        if (!folder.exists() || !folder.isDirectory()) {
            throw new IOException("Path does not exist or is not a directory: " + folderPath);
        }

        File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".parquet"));
        if (files == null || files.length == 0) {
            return 0;
        }

        Configuration conf = new Configuration();
        long totalRows = 0;

        for (File file : files) {
            Path path = new Path(file.getAbsolutePath());
            HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);

            try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                totalRows += reader.getRecordCount();
            }
        }

        return totalRows;
    }

    /**
     * Example usage
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: ParquetFolderReader <folder_path>");
            return;
        }

        String folderPath = args[0];

        try (BufferAllocator allocator = new RootAllocator()) {
            List<VectorSchemaRoot> roots = readFolder(folderPath, allocator);
            System.out.println("Read " + roots.size() + " Parquet files from " + folderPath);

            long totalRows = countRows(folderPath);
            System.out.println("Total rows: " + totalRows);

            // Clean up
            for (VectorSchemaRoot root : roots) {
                root.close();
            }
        }
    }
}
