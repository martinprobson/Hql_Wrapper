package net.martinprobson.hiveutils.hqlwrapper;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


public class FileUtilTest {


	@Test
	public void testExistsPath() {
		File dir = new File(getClass().getResource("/root").getFile());
		assertTrue(FileUtil.exists(new Path(dir.getPath())));
		File notDir = new File(getClass().getResource("/testfile.txt").getFile());
		assertTrue(FileUtil.exists(new Path(notDir.getPath())));
		assertFalse(FileUtil.exists(new Path("fred")));
	}


	@Test
	public void testExistsString() {
		File dir = new File(getClass().getResource("/root").getFile());
		assertTrue(FileUtil.exists(dir.getPath()));
		File notDir = new File(getClass().getResource("/testfile.txt").getFile());
		assertTrue(FileUtil.exists(notDir.getPath()));
		assertFalse(FileUtil.exists("fred"));
	}

	@Test
	public void testIsDirectoryPath() {
		File dir = new File(getClass().getResource("/root").getFile());
		assertTrue(FileUtil.isDirectory(new Path(dir.getPath())));
		File notDir = new File(getClass().getResource("/testfile.txt").getFile());
		assertFalse(FileUtil.isDirectory(new Path(notDir.getPath())));
		
	}


	@Test
	public void testIsDirectoryString() {
		File dir = new File(getClass().getResource("/root").getFile());
		assertTrue(FileUtil.isDirectory(dir.getPath()));
		File notDir = new File(getClass().getResource("/testfile.txt").getFile());
		assertFalse(FileUtil.isDirectory(notDir.getPath()));

	}

	@Test
	public void testListFiles() {
		File dir = new File(getClass().getResource("/root").getFile());
		File[]  exp = dir.listFiles();
		Arrays.sort(exp);
		Path[] paths = FileUtil.listFiles(new Path(dir.getPath()),new PathFilter() {
			public boolean accept(Path name) {
				return (FileUtil.isDirectory(name) || name.getName().toLowerCase().endsWith(".hql"));
			}
		});
		Arrays.sort(paths);
		assertTrue(exp.length == paths.length);
		for (int i= 0; i < exp.length; i++)
			assertTrue(exp[i].getName().equals(paths[i].getName()));
	}

	@Test
	public void testReadFile() throws FileNotFoundException, IOException {
		File test = new File(getClass().getClassLoader().getResource("testfile.txt").getFile());
		String s = FileUtil.readFile(new Path(test.getPath()));
		assertEquals(s,IOUtils.toString(new FileInputStream(test),Charset.defaultCharset()));
	}

	@Test
	public void testReadLocalFile() throws IOException {
		File test = new File(getClass().getClassLoader().getResource("testfile.txt").getFile());
			assertEquals(FileUtil.readLocalFile(test.getAbsolutePath(),Charset.defaultCharset()),
					IOUtils.toString(new FileInputStream(test),Charset.defaultCharset()));
	}

}
