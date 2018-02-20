package net.martinprobson.hiveutils.hqlwrapper;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.junit.Test;

public class UtilTest {

	@Test
	public void testHQLSplit() {
		List<String> stmts = new ArrayList<String>();
		stmts.add("statement 1");
		stmts.add("statement 2");
		stmts.add("statement 3");
		stmts.add("statement 4");
		StringBuilder sb = new StringBuilder();
		for (String s: stmts) sb.append(s).append(";");
		String testString = sb.toString();
		assertTrue(stmts.equals(Util.HQLSplit(testString)));
	}


	@Test
	public void testGetCurrentTimeStamp() {
		assertEquals(Util.getCurrentTimeStamp().substring(0,23),
				     new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss").format(Calendar.getInstance().getTime()).substring(0,23));
	}

	@Test
	public void testSendMailStringStringArrayStringStringStringArray() {
		assertTrue(true);
	}

}
