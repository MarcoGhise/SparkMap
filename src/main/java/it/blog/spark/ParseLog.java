package it.blog.spark;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseLog {
	
	private static final String LOG_ENTRY_PATTERN = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}";

	private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

	public static String parseFromLogLine(String logline) {
		Matcher m = PATTERN.matcher(logline);
		if (m.find()) {
			return m.group(0);
		} else
			return "not valid";

	}
}
