package crate.elasticsearch.export;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import crate.elasticsearch.export.Output.Result;

/**
 * Tests for the @OutputCommand class. These tests currently call
 * UNIX commands and are only executable on Linux/MacOS Systems.
 */
public class OutputCommandTest {

    /**
     * The getOutputstream() method returns null if the command
     * has not been opened yet.
     */
    @Test
    public void testWithoutStart() {
        OutputCommand outputCommand = new OutputCommand("cat", false);
        assertNull(outputCommand.getOutputStream());
    }

    /**
     * A not existing command raises an IO exception when opening.
     */
    @Test
    public void testErrorCommand() {
        OutputCommand outputCommand = new OutputCommand("_notexistingcommand", false);

        // Start the process
        try {
            outputCommand.open();
        } catch (IOException e) {
            // The command does not exist and should raise an IOException
            return;
        }
        fail("Test should raise IOException");
    }

    /**
     * A single command can not have arguments. 'Cat' returns the input
     * to it's output stream. The first 8K of the output stream will be
     * captured by the stream consumer and shown in the result.
     */
    @Test
    public void testSingleCommand() throws IOException {
        // Create a 'cat' output command. The cat command puts
        // the standard in strings to the standard out
        OutputCommand outputCommand = new OutputCommand("cat", false);

        // Start the process
        try {
            outputCommand.open();
        } catch (IOException e) {
            e.printStackTrace();
            fail("Method start() failed due to IOException");
        }

        // Add a bunch of lines to the process' standard in.
        OutputStream os = outputCommand.getOutputStream();
        for (int i = 0; i < 1000000; i++) {
            String string = "Line " + i +"\n";
            os.write(string.getBytes());
        }

        // Finish the process
        outputCommand.close();
        Result result = outputCommand.result();

        // There is a result object
        assertNotNull(result);

        // The exit status of the process is 0
        assertEquals(0, result.exit);

        // There is no error in the standard error log
        assertEquals("", result.stdErr);

        // The first 8K of the standard out are captured
        assertTrue(result.stdOut.endsWith("Line 922\n"));
    }

    /**
     * A list of commands allow arguments to a process. Do a shell script
     * and write to a file.
     */
    @Test
    public void testCommandList() throws IOException {
        // For multiple commands use the list constructor.
        String filename = "/tmp/outputcommand.txt";
        List<String> cmds = Arrays.asList("/bin/sh", "-c", "cat > " + filename);
        OutputCommand outputCommand = new OutputCommand(cmds, false);

        // Start the process
        outputCommand.open();

        // Add a bunch of lines to the process' standard in.
        OutputStream os = outputCommand.getOutputStream();
        for (int i = 0; i < 1000000; i++) {
            String string = "Line " + i +"\n";
            os.write(string.getBytes());
        }

        // Finish the process
        outputCommand.close();
        Result result = outputCommand.result();
        // The exit status of the process is 127
        assertEquals(0, result.exit);

        // The error output is captured
        assertEquals("", result.stdErr);

        // The standard output of the process is empty
        assertEquals("", result.stdOut);

        // The command has been executed and written data to the file
        assertEquals(11888890, new File(filename).length());
        new File(filename).delete();
    }

    /**
     * Writing to a failed/quit process will lead to an IOException
     * as soon as the process is finished.
     */
    @Test
    public void testWriteToFailedCommand() throws IOException {
        OutputCommand outputCommand = new OutputCommand("rm", false);

        // Start the process
        outputCommand.open();

        // Add a bunch of lines to the process' standard in.
        OutputStream os = outputCommand.getOutputStream();
        try {
            for (int i=0; i< 1048576; i++) {
                os.write(65);
            }
        } catch (IOException e) {
            return;
        }
        fail("Writing to failed process did not throw IOException");
    }

    /**
     * A failed command will have an exit status and capture the error log.
     */
    @Test
    public void testFailedCommandResult() throws IOException {
        OutputCommand outputCommand = new OutputCommand("rm", false);

        // Start the process
        outputCommand.open();
        outputCommand.close();
        Result result = outputCommand.result();

        // There is a result object
        assertNotNull(result);

        // The exit status of the process is not 0
        assertTrue(result.exit != 0);

        // There error message is in the standard error log
        assertTrue(result.stdErr.length() > 0);

        // The standard output is empty
        assertEquals("", result.stdOut);
    }

    /**
     * The gzip parameter zips the output to the standard in of the
     * process. The 'gunzip' command unzips the result, and therefore
     * must be readable again.
     */
    @Test
    public void testSingleCommandGZipped() throws IOException {
        // Create a 'cat' output command. The cat command puts
        // the standard in strings to the standard out
        OutputCommand outputCommand = new OutputCommand("gunzip", true);

        // Start the process
        try {
            outputCommand.open();
        } catch (IOException e) {
            e.printStackTrace();
            fail("Method start() failed due to IOException");
        }

        // Add a bunch of lines to the process' standard in.
        OutputStream os = outputCommand.getOutputStream();
        for (int i = 0; i < 1000000; i++) {
            String string = "Line " + i +"\n";
            os.write(string.getBytes());
        }

        // Finish the process
        outputCommand.close();
        Result result = outputCommand.result();

        // There is a result object
        assertNotNull(result);

        // The exit status of the process is 0
        assertEquals(0, result.exit);

        // There is no error in the standard error log
        assertEquals("", result.stdErr);

        // The first 8K of the standard out are captured
        assertTrue(result.stdOut.endsWith("Line 922\n"));
    }
}
