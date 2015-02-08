package com.afconsult.kompetens.bigdata;

import org.apache.crunch.*;
import org.apache.crunch.types.writable.Writables;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.util.ToolRunner;

import java.util.StringTokenizer;

/**
 * Counts frequencies of first and last names.
 *
 */
public class NameCount extends CrunchTool
{
    public static final String INPUT_BASE = "src/main/resources/";
    public static final String INPUT_PEOPLE_CSV = INPUT_BASE + "people.csv";

    public static void main( String[] args ) throws Exception {
        System.exit(ToolRunner.run(new NameCount(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {

        PCollection<String> rawPeople = getPipeline().readTextFile(INPUT_PEOPLE_CSV);

        PTable<String,String> people = rawPeople.parallelDo(new DoFn<String, Pair<String, String>>() {
            @Override
            public void process(String s, Emitter<Pair<String, String>> emitter) {
                StringTokenizer st = new StringTokenizer(s, ",");
                Person p = new Person();
                p.setId(Integer.parseInt(st.nextToken()));
                p.setFirst(st.nextToken());
                p.setLast(st.nextToken());

                if ("Maria".equals(p.getFirst()) && "Johansson".equals(p.getLast())) {
                    emitter.emit(Pair.of(Integer.toString(p.getId()), "Maria Johansson"));
                }
            }
        }, Writables.tableOf(Writables.strings(), Writables.strings()));

        System.out.println(people.materializeToMap().size());

        return getPipeline().done().succeeded() ? 0 : 1;
    }
}
