package com.afconsult.kompetens.bigdata;

import org.apache.crunch.*;
import org.apache.crunch.types.writable.Writables;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.util.ToolRunner;

import java.util.StringTokenizer;

/**
 * Calculates CP data for all names, first and last
 *
 */
public class CorrelatedProbability extends CrunchTool
{
    public static final String INPUT_BASE = "src/main/resources/";
    public static final String INPUT_ACCESS_LOG = INPUT_BASE + "access.log";

    public static void main( String[] args ) throws Exception {
        System.exit(ToolRunner.run(new CorrelatedProbability(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {

        PCollection<String> rawAccessLog = getPipeline().readTextFile(INPUT_ACCESS_LOG);

        PTable<String,String> clicks = rawAccessLog.parallelDo(new DoFn<String, Pair<String,String>>() {
            @Override
            public void process(String s, Emitter<Pair<String,String>> emitter) {

                // IP,userId,timestamp,GET /pdp_PID.html,status,latency
//                String s = String.format("127.0.0.1\\t%d\\t%d\\tGET /pdp_%d.html\\t200\\t152",
//                        userId, timestamp++, pid);
                StringTokenizer st = new StringTokenizer(s, "\t");
                String ip = st.nextToken();
                String userId = st.nextToken();
                String timestamp = st.nextToken();
                String verb = st.nextToken();

                final String prefix = "GET /pdp_";
                final String suffix = ".html";

                String productId = verb.substring(prefix.length(), verb.indexOf(suffix));
                emitter.emit(Pair.of(userId, productId));
            }
        }, Writables.tableOf(Writables.strings(), Writables.strings()));

        getPipeline().writeTextFile(clicks, "target/clicks");

        return getPipeline().done().succeeded() ? 0 : 1;
    }
}
