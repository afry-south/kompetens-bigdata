package com.afconsult.kompetens.bigdata;

import org.apache.crunch.*;
import org.apache.crunch.types.writable.Writables;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.util.ToolRunner;

import java.util.*;

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

        // map pids by user
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

        // reduce: emit user -> pid1:count1, pid2:count2, ...
        PTable<String, Collection<Pair<String,Integer>>> userSessions = clicks.groupByKey().parallelDo(
                new DoFn<Pair<String, Iterable<String>>, Pair<String, Collection<Pair<String,Integer>>>>() {
            @Override
            public void process(Pair<String, Iterable<String>> input, Emitter<Pair<String, Collection<Pair<String,Integer>>>> emitter) {

                final String userId = input.first();
                final Map<String, Integer> productCounts = new TreeMap<String, Integer>();

                for (String pid : input.second()) {
                    Integer count = productCounts.get(pid);
                    productCounts.put(pid, null == count ? 1 : 1 + count);
                }

                Collection<Pair<String,Integer>> sb = new ArrayList<Pair<String, Integer>>();
                for (Map.Entry<String, Integer> entry : productCounts.entrySet()) {
                    sb.add(Pair.of(entry.getKey(), entry.getValue()));
                }

                emitter.emit(Pair.of(userId, sb));
            }
        }, Writables.tableOf(Writables.strings(), Writables.collections(Writables.pairs(Writables.strings(), Writables.ints()))));
        getPipeline().writeTextFile(userSessions, "target/userSessions");

        // map permutations: pid1 -> pid2, pid1 -> pid3
        PTable<String, String> permutations = userSessions.parallelDo(new DoFn<Pair<String, Collection<Pair<String, Integer>>>, Pair<String, String>>() {
            @Override
            public void process(Pair<String, Collection<Pair<String, Integer>>> input, Emitter<Pair<String, String>> emitter) {
                for (Pair<String,Integer> from : input.second()) {
                    for (Pair<String,Integer> to : input.second()) {
                        if (!from.first().equals(to.first())) {
                            emitter.emit(Pair.of(from.first(), to.first()));
                        }
                    }
                }
            }
        }, Writables.tableOf(Writables.strings(), Writables.strings()));
        getPipeline().writeTextFile(permutations, "target/permutations");

        // reduce to correlated probabilities
        PTable<String, Collection<Pair<String,Float>>> correlatedProbabilities = permutations.groupByKey().parallelDo(
                new DoFn<Pair<String, Iterable<String>>, Pair<String, Collection<Pair<String, Float>>>>() {
                    @Override
                    public void process(Pair<String, Iterable<String>> input, Emitter<Pair<String, Collection<Pair<String, Float>>>> emitter) {

                        HashMap<String,Integer> productCounts = new HashMap<String, Integer>();
                        int size = 0;
                        for (String pid : input.second()) {
                            Integer count = productCounts.get(pid);
                            productCounts.put(pid, null == count ? 1 : count+1);
                            size++;
                        }

                        // sort and reverse
                        TreeSet<Pair<String,Float>> sortedSet = new TreeSet<Pair<String, Float>>(SCORED_ENTRIES_COMPARATOR);
                        for (Map.Entry<String,Integer> entry : productCounts.entrySet()) {

                            // small threshold
                            float score = Float.valueOf(entry.getValue()) / size;
                            if (0.007f < score) {
                                sortedSet.add(Pair.of(entry.getKey(), score));
                            }
                        }

                        emitter.emit(Pair.of(input.first(), (Collection<Pair<String, Float>>) sortedSet));
                    }
                },
                Writables.tableOf(Writables.strings(), Writables.collections(Writables.pairs(Writables.strings(), Writables.floats()))));
        getPipeline().writeTextFile(correlatedProbabilities, "target/clickCP");

        return getPipeline().done().succeeded() ? 0 : 1;
    }

    static class ScoredEntriesComparator implements Comparator<Pair<String, Float>> {
        @Override
        public int compare(Pair<String, Float> one, Pair<String, Float> other) {
            return other.second().compareTo(one.second());
        }
    }
    static final ScoredEntriesComparator SCORED_ENTRIES_COMPARATOR = new ScoredEntriesComparator();
}
