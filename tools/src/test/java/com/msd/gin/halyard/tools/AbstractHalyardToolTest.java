package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HBaseServerTestInstance;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public abstract class AbstractHalyardToolTest {

	@Rule
	public final HadoopLogRule hadoopLogs = HadoopLogRule.create();

	protected abstract AbstractHalyardTool createTool();

    @Test
    public void testHelp() throws Exception {
        assertEquals(-1, run(new String[]{"-h"}));
    }

    @Test(expected = MissingOptionException.class)
    public void testRunNoArgs() throws Exception {
        assertEquals(-1, run(new String[]{}));
    }

    @Test
    public void testRunVersion() throws Exception {
        assertEquals(0, run(new String[]{"-v"}));
    }

    @Test(expected = UnrecognizedOptionException.class)
    public void testRunInvalid() throws Exception {
        List<String> args = new ArrayList<>();
        for (Option requiredOpt : createTool().getRequiredOptions()) {
            args.add("-"+requiredOpt.getOpt());
            if (requiredOpt.hasArg()) {
                args.add("foo");
            }
        }
        Set<Character> possibleOpts = new HashSet<>(31);
        for (int i='a'; i<='z'; i++) {
            possibleOpts.add((char)i);
        }
        for (Option opt : createTool().getOptions()) {
            possibleOpts.remove(opt.getOpt().charAt(0));
        }
        args.add("-"+possibleOpts.iterator().next());
        run(args.toArray(new String[args.size()]));
    }

    protected int run(String ... args) throws Exception {
        return run(HBaseServerTestInstance.getInstanceConfig(), args);
    }

    protected int run(Configuration conf, String ... args) throws Exception {
        return ToolRunner.run(conf, createTool(), args);
    }

    private static File getTempDir(String name) throws IOException {
        File dir = File.createTempFile(name, "");
        dir.delete();
        dir.deleteOnExit();
        return dir;
    }

    static File getTempHTableDir(String name) throws IOException {
    	return getTempDir(name);
    }

    static File getTempSnapshotDir(String name) throws IOException {
    	return getTempDir(name);
    }

    static File createTempDir(String name) throws IOException {
        File dir = getTempDir(name);
        dir.mkdir();
        return dir;
    }
}
