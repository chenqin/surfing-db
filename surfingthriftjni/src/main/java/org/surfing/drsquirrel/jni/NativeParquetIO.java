package org.surfing.drsquirrel.jni;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

/**
 * JNI wrapper for native Parquet folder read/write operations.
 * Uses the same native library as NativeThriftDecoder (surfingthriftjni).
 */
public final class NativeParquetIO {
  static {
    // Reuse the same library loading logic as NativeThriftDecoder
    boolean ok = false;
    try {
      System.loadLibrary("surfingthriftjni");
      ok = true;
    } catch (UnsatisfiedLinkError e) {
      // Fallback: attempt explicit load from a configured folder
      String[] candidates = new String[] {
          System.getProperty("native.lib.dir"),
          System.getProperty("java.library.path"),
          new java.io.File("..", "build").getAbsolutePath()
      };
      String lib = System.mapLibraryName("surfingthriftjni");
      for (String dir : candidates) {
        if (dir == null || dir.isEmpty()) continue;
        for (String path : dir.split(java.io.File.pathSeparator)) {
          java.io.File f = new java.io.File(path, lib);
          if (f.exists()) {
            System.load(f.getAbsolutePath());
            ok = true;
            break;
          }
        }
        if (ok) break;
      }
      // Final fallback: attempt to load from a natives jar under META-INF/lib/<os>-<arch>/
      if (!ok) {
        try {
          String os = System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT);
          String arch = System.getProperty("os.arch").toLowerCase(java.util.Locale.ROOT);
          String osNorm;
          if (os.contains("mac") || os.contains("darwin")) osNorm = "osx";
          else if (os.contains("linux")) osNorm = "linux";
          else if (os.contains("win")) osNorm = "windows";
          else osNorm = os;
          String archNorm = arch;
          if ("amd64".equals(archNorm)) archNorm = "x86_64";
          if ("x86-64".equals(archNorm)) archNorm = "x86_64";
          if ("aarch64".equals(archNorm)) archNorm = "aarch_64";
          String mapped = System.mapLibraryName("surfingthriftjni");
          String resPath = String.format("/META-INF/lib/%s-%s/%s", osNorm, archNorm, mapped);
          try (java.io.InputStream in = NativeParquetIO.class.getResourceAsStream(resPath)) {
            if (in != null) {
              java.nio.file.Path tmp = java.nio.file.Files.createTempFile("surfingthriftjni-", mapped);
              tmp.toFile().deleteOnExit();
              java.nio.file.Files.copy(in, tmp, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
              System.load(tmp.toAbsolutePath().toString());
              ok = true;
            }
          }
        } catch (Throwable t) {
          // ignore, fall through to throw original error
        }
      }
      if (!ok) throw e;
    }
  }

  private NativeParquetIO() {}

  /**
   * Native method to read all Parquet files from a folder.
   * @param folderPath Directory containing .parquet files
   * @param schemaOutAddr Address of ArrowSchema output
   * @param arrayOutAddr Address of ArrowArray output
   * @throws IOException If reading fails
   */
  private static native void readFolder(String folderPath,
                                        long schemaOutAddr, long arrayOutAddr) throws IOException;

  /**
   * Native method to write Arrow data to Parquet folder.
   * @param schemaAddr Address of ArrowSchema input
   * @param arrayAddr Address of ArrowArray input
   * @param folderPath Output directory path
   * @param prefix Filename prefix (e.g., "part")
   * @param numFiles Number of files to create
   * @throws IOException If writing fails
   */
  private static native void writeFolder(long schemaAddr, long arrayAddr,
                                         String folderPath, String prefix, int numFiles) throws IOException;

  /**
   * Read all Parquet files from a directory into a VectorSchemaRoot.
   *
   * @param allocator Arrow buffer allocator
   * @param folderPath Directory containing .parquet files
   * @return VectorSchemaRoot with combined data from all files
   * @throws IOException If reading fails
   */
  public static VectorSchemaRoot readParquetFolder(BufferAllocator allocator,
                                                   String folderPath) throws IOException {
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
         ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      readFolder(folderPath, outSchema.memoryAddress(), outArray.memoryAddress());
      return Data.importVectorSchemaRoot(allocator, outArray, outSchema, null);
    }
  }

  /**
   * Write a VectorSchemaRoot to multiple Parquet files in a directory.
   *
   * @param allocator Arrow buffer allocator
   * @param root Arrow data to write
   * @param folderPath Output directory path
   * @param prefix Filename prefix (default: "part")
   * @param numFiles Number of files to create (data will be split evenly)
   * @throws IOException If writing fails
   */
  public static void writeParquetFolder(BufferAllocator allocator,
                                        VectorSchemaRoot root,
                                        String folderPath,
                                        String prefix,
                                        int numFiles) throws IOException {
    try (ArrowSchema schema = ArrowSchema.allocateNew(allocator);
         ArrowArray array = ArrowArray.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, null, array, schema);
      writeFolder(schema.memoryAddress(), array.memoryAddress(),
                  folderPath, prefix, numFiles);
    }
  }

  /**
   * Convenience method with default prefix and single file.
   */
  public static void writeParquetFolder(BufferAllocator allocator,
                                        VectorSchemaRoot root,
                                        String folderPath) throws IOException {
    writeParquetFolder(allocator, root, folderPath, "part", 1);
  }
}
