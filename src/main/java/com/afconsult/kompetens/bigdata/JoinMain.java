package com.afconsult.kompetens.bigdata;

import org.apache.crunch.*;
import org.apache.crunch.lib.Channels;
import org.apache.crunch.types.writable.Writables;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.util.ToolRunner;

import java.util.StringTokenizer;

/**
 * Hello world!
 *
 */
public class JoinMain extends CrunchTool
{
    public static final String INPUT_BASE = "src/main/resources/";
    public static final String INPUT_PEOPLE_CSV = INPUT_BASE + "people.csv";

    public static void main( String[] args ) throws Exception {
        System.exit(ToolRunner.run(new JoinMain(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {

        PCollection<String> rawPeople = getPipeline().readTextFile(INPUT_PEOPLE_CSV);

        PCollection<Pair<String,String>> people = rawPeople.parallelDo(new DoFn<String, Pair<String, String>>() {
            @Override
            public void process(String s, Emitter<Pair<String, String>> emitter) {
                StringTokenizer st = new StringTokenizer(s, ",");
                Person p = new Person();
                p.setId(Integer.parseInt(st.nextToken()));
                p.setFirst(st.nextToken());
                p.setLast(st.nextToken());

                emitter.emit(Pair.of(p.getFirst(), p.getLast()));
            }
        }, Writables.pairs(Writables.strings(), Writables.strings()));

        // do the split
        Pair<PCollection<String>, PCollection<String>> split = Channels.split(people);

        // Simple Aggregation in Crunch
        PTable<String, Long> fqFirst = split.first().count();
        PTable<String, Long> fqLast = split.second().count();

        getPipeline().writeTextFile(fqFirst.top(20), "target/firstNames-top-20");
        getPipeline().writeTextFile(fqLast.bottom(10), "target/lastNames-bottom-10");

        return getPipeline().done().succeeded() ? 0 : 1;
    }
}
