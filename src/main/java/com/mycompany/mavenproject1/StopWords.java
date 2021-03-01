/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenproject1;

import opennlp.tools.cmdline.PerformanceMonitor;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;


/**
 * @author kwstas, angelo, teo
 */

public class StopWords {

    static Set<String> nounPhrases = new HashSet<>();

    public static void main(String[] args) throws IOException, URISyntaxException {
        // int k = 3;
        int N = Integer.parseInt(args[3]); // N = megethos parathirou 3<=n<=10
        int k = Integer.parseInt(args[4]); // k = poses lekseis tha mas diksei to pagerank

        SparkConf sparkConf = new SparkConf().setAppName("StopWords");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // FileSystem fs = FileSystem.get(sc);
        // List<String> fileslist = listAllFilePath(args[0],fs);

        // pernei to configuration instance g to hdfs
        Configuration configuration = new Configuration();
        // pernei to instance tou HDFS
        FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:54310"), configuration);
        // pernei ta metadata tou fakelu sto args[0]
        FileStatus[] fileStatus = hdfs.listStatus(new Path(args[0]));
        // xrisimopoiei tin FileUtil gia na parei ta paths ton arxeion
        Path[] paths = FileUtil.stat2Paths(fileStatus);
        // kai pernei ena ena ta arxeia k ta vazei se mia lista apo paths
        List<Path> ListOfFiles = new ArrayList<>();
        for (Path path : paths) {
            ListOfFiles.add(path);
            // System.out.println(path);
        }
        // diavazei kathe path-arxeio stin lista
        for (Path file : ListOfFiles) {
            List<String> myWords = new ArrayList<>(); // lista me lekseis p.x nouns,objects
            List<String> neighbors_List = new ArrayList<>(); // lista pou ta vazei se tuples
            String fileName = file.getName(); // pernei to onoma tou arxeiou
            JavaRDD<String> lines = sc.textFile(file.toString()); // pernaei oles tis grammes tou sigkekrimenu file
            lines.cache(); // cache afou tha xrisimopoiithun argotera ksana oi lines

            // String tokens[];
            System.out.println("----------------------------------------------------------");
            System.out.println("----dddddddddddddddddddd---------------");
          //  lines.collect().foreach(line->{System.out.println(line);});
            //lines.map(line ->(line ,line.length));
            for(String line:lines.collect()){
            
                /*
                 * pernei mia mia tis grammes tis spaei se lekseis, kai apofasizei ama tha
                 * kratisei tin kathe leksi ksexorista
                 */
                System.out.println("----------------------------------------------------------");
                System.out.println(line);
                InputStream modelIn = null;
                POSModel POSModel = null;
                File modelFile = new File("en-pos-maxent.bin");

                if (!modelFile.exists()) {
                    System.out.println("Downloading model.");
                    try {
                        download("http://opennlp.sourceforge.net/models-1.5/en-pos-maxent.bin", modelFile);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                POSModel model = null;
                try {
                    model = new POSModel(modelFile);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                PerformanceMonitor perfMon = new PerformanceMonitor(System.err, "sent");
                POSTaggerME tagger = new POSTaggerME(model);
                SimpleTokenizer tokenizer = new SimpleTokenizer(); // spazei se lekseis

                // System.out.println("* "+line);
                String[] tokens = tokenizer.tokenize(line);
                int flag = 0;
                String[] tagged = tagger.tag(tokens);
                int size = tagged.length;
                for (int i = 0; i < tagged.length; i++) {
                    if (tagged[i].equalsIgnoreCase("nn") || tagged[i].equalsIgnoreCase("jj")) { // edw koitazume ama
                        // i leksi einai
                        // noun i adjective
                        // System.out.println(tokens[i]);
                        myWords.add(tokens[i]);
                    }
                }

            }
            JavaRDD<String> myWords1 = sc.parallelize(myWords,1);
            //for(String word:myWords1.collect()){ 
            for (int i = 0; i < myWords.size(); i++) {
                /*
                 * koitaei poso makria einai i mia leksi apo tin ali me vasi to megethos tou
                 * parathiru , oste na ftiaksei tin lista me tous gitones tis kathe leksis
                 */
                for (int l = 1; l < N; l++) {

                    if (i + l < myWords.size()) {
                         neighbors_List.add(myWords.get(i) + " " + myWords.get(i + l));
                    } else {
                         break;
                    }
                }

            }

            // ftiaxnei to neighborsRDD me tis lekseis pou ksexorisan sto proigumeno vima.
            JavaRDD<String> neighborsRDD = sc.parallelize(neighbors_List, 1);
            neighborsRDD.cache();
            // ftiaxnei tis akmes tou grafimatos ,ksexorizei tis lekseis me vasi ta kena,
            // kai ftiaxnei ta zeugaria
            JavaPairRDD<String, String> edges = neighborsRDD.mapToPair((s) -> {

                String[] n = s.split("\\s+");
                return new Tuple2<>(n[0], n[1]);
            }).distinct();

            JavaPairRDD<String, Iterable<String>> adjacencyList = edges.groupByKey();

            // cache afu xreiazete g iterations
            adjacencyList.cache();

            // initialize rank to 1.0 for each url in adjacency list // dinw times stous
            // komvous aso
            // to key sto adjancecylist einai i komvoi
            JavaPairRDD<String, Double> ranks = adjacencyList.mapValues(it -> 1.0); // tha vgalw ena idio rdd me ton
            // komvo kai to asso
            ranks.cache(); // afou tha xriastun gia ta polla iterations kalitera na filaxthun stin cache
            // gia auto kane mapvaluse gia na alla3w mono to values ki oci to key
            // int iterations = Integer.parseInt(args[2]);
            // int iterations = 2;
            int iterations = Integer.parseInt(args[2]);

            // 8elw na upologisw pio einai to contribution tou la8e komvou stous geitones
            // tou
            // kanw join sth prwth sthlh sto urlneigboursandrank gia na exw 1,<<2,3,4>,1.0>
            for (int i = 0; i < iterations; i++) {

                // join rank of each url with neighbors
                JavaPairRDD<String, Tuple2<Iterable<String>, Double>> urlNeighborsAndRank = adjacencyList.join(ranks);

                // compute rank contribution for each url , apo ta values to kanw
                // flatmaptopair(t)[t=<<2,3,4>,1.0>>] apo auto
                // 8elw na vgalw polla <2,1.0*1/3> ston 2 komvo steile toso px <3,1.0*1/3>
                // <4,1.0*1/3>
                // kai an <<1,5>,1.0> ston <1,1/0*1/2> kai <5,1.0*1/2>
                JavaPairRDD<String, Double> urlContributions = urlNeighborsAndRank.values().flatMapToPair((t) -> {

                    Iterable<String> neighbors = t._1();
                    Double rank = t._2;

                    // count total neighbors using Iterables.size() method
                    int count = 0;
                    for (String s : neighbors) {
                        count++; // 3 an <<2,3,4>,1.0>
                    }
                    // mia lista pou vazume tin leksi kai dipla to ranking tis
                    List<Tuple2<String, Double>> results = new ArrayList<>();

                    // iterate over all neighbors of a node and output contribution
                    // for that node which is rank / number-of_neighbors

                    for (String s : neighbors) {
                        results.add(new Tuple2<>(s, rank / count));// prosoxi na mhn exw 0 geitones
                    }

                    return results.iterator();

                });

                // sum all contributions for a url o komvos kai olh h pi8anothta pou erxetai apo
                // tous geitones
                JavaPairRDD<String, Double> urlAllContributions = urlContributions.reduceByKey((a, b) -> a + b);

                // recalculate rank from contribution v as 0.15 + v * 0.85
                ranks = urlAllContributions.mapValues(s -> 0.15 + s * 0.85);

            }

            // allazoume tin thesi tis leksis me to rank tis, gia na kanume sort me vasi to
            // key
            JavaPairRDD<Double, String> swapped = ranks
                    .flatMapToPair(item -> Collections.singletonList(item.swap()).iterator());
            swapped.collect();

            JavaPairRDD<Double, String> sorted = swapped.sortByKey(false);
            List<String> words = sorted.values().take(k); // pernume tis k lekseis p thelume
            String storepath = args[1] + "/" + fileName; // storepath

            Path hdfsWritePath = new Path(storepath); // pernei to path tu arxeiou sto opoio tha grapsi
            FSDataOutputStream fsDataOutputStream = hdfs.create(hdfsWritePath, true); // to dimiourgei
            BufferedWriter bufferedWriter = new BufferedWriter(
                    new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8)); // ftiaxnei ton buffer o
            // opooios tha grapsei sto
            // arxeio
            for (String str : words) {
                bufferedWriter.write(str); // grafei ston buffer mia mia tis lekseis apo to words
                bufferedWriter.newLine();
            }
            bufferedWriter.close(); // kleinei ton buffer

        }
        hdfs.close(); // sto telos klinei kai to conf tou hdfs
        sc.stop();

    }

    private static void download(String url, File destination) throws IOException {
        URL website = new URL(url);
        ReadableByteChannel rbc = Channels.newChannel(website.openStream());
        FileOutputStream fos = new FileOutputStream(destination);
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        fos.close();
    }

}