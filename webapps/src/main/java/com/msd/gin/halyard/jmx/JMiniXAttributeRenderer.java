package com.msd.gin.halyard.jmx;

import java.text.DateFormat;
import java.util.Date;

import org.jminix.type.AttributeFilter;
import org.jminix.type.HtmlContent;

import com.msd.gin.halyard.sail.HBaseSail;

public class JMiniXAttributeRenderer implements AttributeFilter {

	@Override
	public Object filter(Object object) {
		if (object instanceof HBaseSail.QueryInfo[]) {
			HBaseSail.QueryInfo[] arr = (HBaseSail.QueryInfo[]) object;
			StringBuilder s = new StringBuilder();
			s.append("<ol>");
			for (HBaseSail.QueryInfo qi : arr) {
				s.append("<li>");
				s.append("<p>timestamp: ").append(DateFormat.getDateTimeInstance().format(new Date(qi.getTimestamp()))).append("</p>");
				s.append("<p>queryString: <pre>").append(qi.getQueryString()).append("</pre></p>");
				s.append("<p>queryTree: <pre>").append(qi.getQueryTree()).append("</pre></p>");
				s.append("<p>optimizedQueryTree: <pre>").append(qi.getOptimizedQueryTree()).append("</pre></p>");
				s.append("</li>");
			}
			s.append("</ol>");
			return new Html(s.toString());
		} else {
			return object;
		}
	}

	public static final class Html implements HtmlContent {
		private final String html;
		public Html(String html) {
			this.html = html;
		}
		@Override
		public String toString() {
			return html;
		}
	}
}
