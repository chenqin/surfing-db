package com.pinterest.surfing.parquet;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for writing Arrow VectorSchemaRoot data to Parquet files in a directory.
 */
public class ParquetFolderWriter {

    /**
     * Write a list of VectorSchemaRoots to Parquet files in a directory.
     * Each VectorSchemaRoot is written to a separate file.
     *
     * @param roots      List of VectorSchemaRoot to write
     * @param folderPath The output directory
     * @param prefix     File name prefix (default: "part")
     * @return List of written file paths
     * @throws IOException If writing fails
     */
    public static List<String> writeFolder(List<VectorSchemaRoot> roots, String folderPath, String prefix) throws IOException {
        List<String> writtenFiles = new ArrayList<>();

        if (roots == null || roots.isEmpty()) {
            return writtenFiles;
        }

        File folder = new File(folderPath);
        if (!folder.exists()) {
            folder.mkdirs();
        }

        Configuration conf = new Configuration();

        for (int i = 0; i < roots.size(); i++) {
            VectorSchemaRoot root = roots.get(i);
            if (root == null || root.getRowCount() == 0) {
                continue;
            }

            String fileName = String.format("%s_%05d.parquet", prefix, i);
            String filePath = new File(folder, fileName).getAbsolutePath();

            writeParquetFile(root, filePath, conf);
            writtenFiles.add(filePath);
        }

        return writtenFiles;
    }

    /**
     * Write a list of VectorSchemaRoots to Parquet files with default prefix.
     *
     * @param roots      List of VectorSchemaRoot to write
     * @param folderPath The output directory
     * @return List of written file paths
     * @throws IOException If writing fails
     */
    public static List<String> writeFolder(List<VectorSchemaRoot> roots, String folderPath) throws IOException {
        return writeFolder(roots, folderPath, "part");
    }

    /**
     * Write a single VectorSchemaRoot to a Parquet file.
     *
     * @param root     The VectorSchemaRoot to write
     * @param filePath The output file path
     * @param conf     Hadoop configuration
     * @throws IOException If writing fails
     */
    public static void writeParquetFile(VectorSchemaRoot root, String filePath, Configuration conf) throws IOException {
        if (root == null || root.getRowCount() == 0) {
            throw new IOException("Empty or null VectorSchemaRoot");
        }

        // Use JNI implementation for better compatibility
        try {
            File file = new File(filePath);
            String folderPath = file.getParent();
            String fileName = file.getName().replace(".parquet", "");

            org.apache.arrow.memory.BufferAllocator allocator = root.getFieldVectors().get(0).getAllocator();
            com.pinterest.drsquirrel.jni.NativeParquetIO.writeParquetFolder(allocator, root, folderPath, fileName, 1);
        } catch (UnsatisfiedLinkError e) {
            throw new IOException("Native Parquet writer not available. Please ensure libsurfingthriftjni.so is in java.library.path", e);
        } catch (Exception e) {
            throw new IOException("Failed to write Parquet file: " + filePath, e);
        }
    }

    /**
     * Write a single VectorSchemaRoot to a Parquet file with default configuration.
     *
     * @param root     The VectorSchemaRoot to write
     * @param filePath The output file path
     * @throws IOException If writing fails
     */
    public static void writeParquetFile(VectorSchemaRoot root, String filePath) throws IOException {
        writeParquetFile(root, filePath, new Configuration());
    }

    /**
     * Example usage
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: ParquetFolderWriter <output_folder>");
            System.out.println("This will create sample Parquet files for demonstration.");
            return;
        }

        String outputFolder = args[0];

        // Create sample data (you would replace this with actual VectorSchemaRoots)
        org.apache.arrow.memory.BufferAllocator allocator = new org.apache.arrow.memory.RootAllocator();

        try {
            // Example: Create a simple schema and data
            org.apache.arrow.vector.types.pojo.Field field1 =
                org.apache.arrow.vector.types.pojo.Field.nullable("id", new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true));
            org.apache.arrow.vector.types.pojo.Field field2 =
                org.apache.arrow.vector.types.pojo.Field.nullable("name", new org.apache.arrow.vector.types.pojo.ArrowType.Utf8());

            Schema schema = new Schema(java.util.Arrays.asList(field1, field2));
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

            org.apache.arrow.vector.BigIntVector idVector = (org.apache.arrow.vector.BigIntVector) root.getVector("id");
            org.apache.arrow.vector.VarCharVector nameVector = (org.apache.arrow.vector.VarCharVector) root.getVector("name");

            // Populate with sample data
            root.setRowCount(3);
            idVector.setSafe(0, 1L);
            idVector.setSafe(1, 2L);
            idVector.setSafe(2, 3L);
            nameVector.setSafe(0, "Alice".getBytes());
            nameVector.setSafe(1, "Bob".getBytes());
            nameVector.setSafe(2, "Charlie".getBytes());

            List<VectorSchemaRoot> roots = new ArrayList<>();
            roots.add(root);

            List<String> files = writeFolder(roots, outputFolder);
            System.out.println("Wrote " + files.size() + " Parquet file(s) to " + outputFolder);
            for (String file : files) {
                System.out.println("  - " + file);
            }

            root.close();
        } finally {
            allocator.close();
        }
    }
}
