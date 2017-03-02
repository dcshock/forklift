package forklift;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.kohsuke.args4j.ExampleMode.ALL;

/**
 * Created by afrieze on 11/4/16.
 */
public class ForkliftMain {

    /**
     * Launch a Forklift server instance.
     */
    public static void main(String[] args) throws Throwable {
        final ForkliftOpts opts = new ForkliftOpts();
        final CmdLineParser argParse = new CmdLineParser(opts);
        try {
            argParse.parseArgument(args);
        } catch (CmdLineException e) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            System.err.println("java SampleMain [options...] arguments...");
            // print the list of available options
            argParse.printUsage(System.err);
            System.err.println();
            // print option sample. This is useful some time
            System.err.println("  Example: java SampleMain" + argParse.printExample(ALL));

            return;
        }

        File f = new File(opts.getConsumerDir());
        if (!f.exists() || !f.isDirectory()) {
            System.err.println();
            System.err.println(" -monitor1 is not a valid directory.");
            System.err.println();
            argParse.printUsage(System.err);
            System.err.println();
            return;
        }
        ForkliftServer server = new ForkliftServer(opts);
        server.startServer(20, TimeUnit.SECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    server.stopServer(10, TimeUnit.SECONDS);
                } catch(InterruptedException e){
                    e.printStackTrace(System.out);
                }
            }
        });
    }

}
