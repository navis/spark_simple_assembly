/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.skt.spark;

import org.apache.maven.plugins.shade.relocation.Relocator;
import org.codehaus.plexus.util.SelectorUtils;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.commons.Remapper;
import org.objectweb.asm.commons.RemappingClassAdapter;


import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.zip.ZipEntry;

public class SimpleAssembly {
  private static final String[] CLASSES = new String[]{
      "assembly/target",
      "bagel/target",
      "core/target",
//      "docker-integration-tests/target",
//      "examples/target",
//      "external/flume/target",
//      "external/flume-assembly/target",
//      "external/flume-sink/target",
//      "external/kafka/target",
//      "external/kafka-assembly/target",
//      "external/mqtt/target",
//      "external/mqtt-assembly/target",
//      "external/twitter/target",
//      "external/zeromq/target",
//      "extras/kinesis-asl/target",
//      "extras/kinesis-asl-assembly/target",
//      "extras/spark-ganglia-lgpl/target",
      "graphx/target",
      "launcher/target",
      "mllib/target",
      "network/common/target",
      "network/shuffle/target",
      "network/yarn/target",
      "project/project/target",
      "project/target",
      "repl/target",
      "sql/catalyst/target",
      "sql/core/target",
      "sql/hive/target",
      "sql/hive-thriftserver/target",
      "sql/target",
      "streaming/target",
//      "tags/target",
      "target",
//      "tools/target",
      "unsafe/target",
      "yarn/target"
  };
  /**
   * <relocation>
   * <pattern>org.eclipse.jetty</pattern>
   * <shadedPattern>org.spark-project.jetty</shadedPattern>
   * <includes>
   * <include>org.eclipse.jetty.**</include>
   * </includes>
   * </relocation>
   * <relocation>
   * <pattern>com.google.common</pattern>
   * <shadedPattern>org.spark-project.guava</shadedPattern>
   * <excludes>
   * <!--
   * These classes cannot be relocated, because the Java API exposes the
   * "Optional" type; the others are referenced by the Optional class.
   * -->
   * <exclude>com/google/common/base/Absent*</exclude>
   * <exclude>com/google/common/base/Function</exclude>
   * <exclude>com/google/common/base/Optional*</exclude>
   * <exclude>com/google/common/base/Present*</exclude>
   * <exclude>com/google/common/base/Supplier</exclude>
   * </excludes>
   * </relocation>
   */
  private static final RelocatorRemapper REMAPPER = new RelocatorRemapper(Arrays.<Relocator>asList(
      new SimpleRelocator("org.eclipse.jetty", "org.spark-project.jetty",
          Arrays.asList("org.eclipse.jetty.**"), null),
      new SimpleRelocator("com.google.common", "org.spark-project.guava",
          null, Arrays.asList("com/google/common/base/Absent*", "com/google/common/base/Function",
          "com/google/common/base/Optional*", "com/google/common/base/Present*", "com/google/common/base/Supplier"))
  ));

  private static final String[] LOC = new String[]{"scala-2.10/classes", "classes"};

  private static final HashMap<String, File> root = new LinkedHashMap<String, File>();

  private static void make(File f) {
    File[] children = f.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isFile()) {
        String absolutePath = child.getAbsolutePath();
        String name = absolutePath.substring(absolutePath.indexOf("classes/") + 8);
        root.put(name, child);
      } else {
        make(child);
      }
    }
  }

  public static void assemble(File oldFile, File newFile) throws Exception {

    System.out.println("-- oldFile = " + oldFile);
    System.out.println("-- newFile = " + newFile);

    long start = System.currentTimeMillis();
    int total = 0;
    int updated = 0;
    int added = 0;

    JarFile source = new JarFile(oldFile);
    ZipEntry dummy = new ZipEntry("dummy");

    JarOutputStream newAssembly = new JarOutputStream(new BufferedOutputStream(new FileOutputStream(newFile), 65536));

    Enumeration<JarEntry> entries = source.entries();
    while (entries.hasMoreElements()) {
      total++;
      JarEntry entry = entries.nextElement();

      String name = entry.getName();
      File classFile = root.remove(name);

      if (classFile != null) {
        dummy.setTime(classFile.lastModified());
      }

      if (classFile == null || entry.getTime() >= dummy.getTime()) {
        newAssembly.putNextEntry(entry);
        copyBytes(source.getInputStream(entry), newAssembly);
      } else {
        updated++;
        System.out.println("-- updating.. " + name +
            " assembly (" + new Date(entry.getTime()) + ":" + entry.getSize() + ")" +
            " current (" + new Date(classFile.lastModified()) + ":" + classFile.length() + ")");
        if (name.endsWith(".class")) {
          addRemappedClass(REMAPPER, newAssembly, name, new FileInputStream(classFile));
        } else {
          InputStream input = new FileInputStream(classFile);

          entry = new JarEntry(name);
          entry.setTime(classFile.lastModified());
          entry.setSize(classFile.length());

          newAssembly.putNextEntry(entry);
          copyBytes(source.getInputStream(entry), newAssembly);
          input.close();
        }
      }
    }
    for (Map.Entry<String, File> entry : root.entrySet()) {
      added++; total++;
      System.out.println("-- adding.. " + entry);
      ZipEntry newEntry = new ZipEntry(entry.getKey());
      File missedFile = entry.getValue();
      newEntry.setTime(missedFile.lastModified());
      newEntry.setSize(missedFile.length());

      InputStream input = new FileInputStream(missedFile);
      newAssembly.putNextEntry(newEntry);
      copyBytes(input, newAssembly);
      input.close();
    }
    source.close();
    newAssembly.close();

    System.out.println("++ done.. took " + (System.currentTimeMillis() - start) + " msec, " +
        "total " + total + " entries, updated " + updated + " entries, added " + added + " entries");
  }

  private static final byte buf[] = new byte[65536];

  private static void copyBytes(InputStream in, OutputStream out) throws IOException {
    int bytesRead = in.read(buf);
    while (bytesRead >= 0) {
      out.write(buf, 0, bytesRead);
      bytesRead = in.read(buf);
    }
  }

  public static void main(String[] args) throws Exception {
    String SPARK_HOME = System.getenv("SPARK_HOME");
    File input = null;
    if (args.length == 0) {
      File[] files = new File("assembly/target/scala-2.10/").listFiles();
      if (files == null || files.length == 0) {
        throw new IllegalArgumentException("input assembly is not available");
      }
      for (File file : files) {
        if (input == null || input.lastModified() < file.lastModified()) {
          input = file;
        }
      }
    } else {
      input = Paths.get(args[0]).toFile();
    }
    if (input == null || !input.exists()) {
      throw new IllegalArgumentException("input assembly is not available");
    }

    File renamed = new File(input.getParent(), input.getName() + ".bak");
    renamed.delete();

    input.renameTo(renamed);

    for (String root : CLASSES) {
      File parent = new File(SPARK_HOME, root);
      for (String appendix : LOC) {
        make(new File(parent, appendix));
      }
    }
    assemble(renamed, input);
  }

  // belows are copied from maven-shade plugin
  private static void addRemappedClass(RelocatorRemapper remapper, JarOutputStream jos, String name, InputStream is)
      throws IOException {

    ClassReader cr = new ClassReader(is);

    // We don't pass the ClassReader here. This forces the ClassWriter to rebuild the constant pool.
    // Copying the original constant pool should be avoided because it would keep references
    // to the original class names. This is not a problem at runtime (because these entries in the
    // constant pool are never used), but confuses some tools such as Felix' maven-bundle-plugin
    // that use the constant pool to determine the dependencies of a class.
    ClassWriter cw = new ClassWriter(0);

    final String pkg = name.substring(0, name.lastIndexOf('/') + 1);
    ClassVisitor cv = new RemappingClassAdapter(cw, remapper) {
      @Override
      public void visitSource(final String source, final String debug) {
        if (source == null) {
          super.visitSource(source, debug);
        } else {
          final String fqSource = pkg + source;
          final String mappedSource = remapper.map(fqSource);
          final String filename = mappedSource.substring(mappedSource.lastIndexOf('/') + 1);
          super.visitSource(filename, debug);
        }
      }
    };

    try {
      cr.accept(cv, ClassReader.EXPAND_FRAMES);
    } catch (Throwable ise) {
      throw new IllegalStateException("Error in ASM processing class " + name, ise);
    }

    byte[] renamedClass = cw.toByteArray();

    // Need to take the .class off for remapping evaluation
    String mappedName = remapper.map(name.substring(0, name.indexOf('.')));

    // Now we put it back on so the class file is written out with the right extension.
    jos.putNextEntry(new JarEntry(mappedName + ".class"));
    jos.write(renamedClass);
  }

  private static class SimpleRelocator implements Relocator {

    private final String pattern;

    private final String pathPattern;

    private final String shadedPattern;

    private final String shadedPathPattern;

    private final Set<String> includes;

    private final Set<String> excludes;

    private final boolean rawString;

    public SimpleRelocator(String patt, String shadedPattern, List<String> includes, List<String> excludes) {
      this(patt, shadedPattern, includes, excludes, false);
    }

    public SimpleRelocator(String patt, String shadedPattern, List<String> includes, List<String> excludes,
                           boolean rawString) {
      this.rawString = rawString;

      if (rawString) {
        this.pathPattern = patt;
        this.shadedPathPattern = shadedPattern;

        this.pattern = null; // not used for raw string relocator
        this.shadedPattern = null; // not used for raw string relocator
      } else {
        if (patt == null) {
          this.pattern = "";
          this.pathPattern = "";
        } else {
          this.pattern = patt.replace('/', '.');
          this.pathPattern = patt.replace('.', '/');
        }

        if (shadedPattern != null) {
          this.shadedPattern = shadedPattern.replace('/', '.');
          this.shadedPathPattern = shadedPattern.replace('.', '/');
        } else {
          this.shadedPattern = "hidden." + this.pattern;
          this.shadedPathPattern = "hidden/" + this.pathPattern;
        }
      }

      this.includes = normalizePatterns(includes);
      this.excludes = normalizePatterns(excludes);
    }

    private Set<String> normalizePatterns(Collection<String> patterns) {
      Set<String> normalized = null;

      if (patterns != null && !patterns.isEmpty()) {
        normalized = new LinkedHashSet<String>();

        for (String pattern : patterns) {

          String classPattern = pattern.replace('.', '/');

          normalized.add(classPattern);

          if (classPattern.endsWith("/*")) {
            String packagePattern = classPattern.substring(0, classPattern.lastIndexOf('/'));
            normalized.add(packagePattern);
          }
        }
      }

      return normalized;
    }

    private boolean isIncluded(String path) {
      if (includes != null && !includes.isEmpty()) {
        for (String include : includes) {
          if (SelectorUtils.matchPath(include, path, true)) {
            return true;
          }
        }
        return false;
      }
      return true;
    }

    private boolean isExcluded(String path) {
      if (excludes != null && !excludes.isEmpty()) {
        for (String exclude : excludes) {
          if (SelectorUtils.matchPath(exclude, path, true)) {
            return true;
          }
        }
      }
      return false;
    }

    public boolean canRelocatePath(String path) {
      if (rawString) {
        return Pattern.compile(pathPattern).matcher(path).find();
      }

      if (path.endsWith(".class")) {
        path = path.substring(0, path.length() - 6);
      }

      if (!isIncluded(path) || isExcluded(path)) {
        return false;
      }

      // Allow for annoying option of an extra / on the front of a path. See MSHADE-119; comes from
      // getClass().getResource("/a/b/c.properties").
      return path.startsWith(pathPattern) || path.startsWith("/" + pathPattern);
    }

    public boolean canRelocateClass(String clazz) {
      return !rawString && clazz.indexOf('/') < 0 && canRelocatePath(clazz.replace('.', '/'));
    }

    public String relocatePath(String path) {
      if (rawString) {
        return path.replaceAll(pathPattern, shadedPathPattern);
      } else {
        return path.replaceFirst(pathPattern, shadedPathPattern);
      }
    }

    public String relocateClass(String clazz) {
      return clazz.replaceFirst(pattern, shadedPattern);
    }

    public String applyToSourceContent(String sourceContent) {
      if (rawString) {
        return sourceContent;
      } else {
        return sourceContent.replaceAll("\\b" + pattern, shadedPattern);
      }
    }
  }

  private static class RelocatorRemapper extends Remapper {

    private final Pattern classPattern = Pattern.compile("(\\[*)?L(.+);");

    List<Relocator> relocators;

    public RelocatorRemapper(List<Relocator> relocators) {
      this.relocators = relocators;
    }

    public Object mapValue(Object object) {
      if (object instanceof String) {
        String name = (String) object;
        String value = name;

        String prefix = "";
        String suffix = "";

        Matcher m = classPattern.matcher(name);
        if (m.matches()) {
          prefix = m.group(1) + "L";
          suffix = ";";
          name = m.group(2);
        }

        for (Relocator r : relocators) {
          if (r.canRelocateClass(name)) {
            value = prefix + r.relocateClass(name) + suffix;
            break;
          } else if (r.canRelocatePath(name)) {
            value = prefix + r.relocatePath(name) + suffix;
            break;
          }
        }

        return value;
      }

      return super.mapValue(object);
    }

    public String map(String name) {
      String value = name;

      String prefix = "";
      String suffix = "";

      Matcher m = classPattern.matcher(name);
      if (m.matches()) {
        prefix = m.group(1) + "L";
        suffix = ";";
        name = m.group(2);
      }

      for (Relocator r : relocators) {
        if (r.canRelocatePath(name)) {
          value = prefix + r.relocatePath(name) + suffix;
          break;
        }
      }

      return value;
    }
  }
}
