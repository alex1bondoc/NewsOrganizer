import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class NewsOrganizer {
    // per-thread partial results used for reports.txt
    static int[] total = null;
    static int[] cnt = null;
    // barrier separating the processing stages
    static CyclicBarrier barrier;
    // categories
    static ConcurrentHashMap<String, List<String>> categories = new ConcurrentHashMap<>();
    static Set<String> categoriesSet = ConcurrentHashMap.newKeySet();
    // languages
    static ConcurrentHashMap<String, List<String>> languages = new ConcurrentHashMap<>();
    static Set<String> languagesSet = ConcurrentHashMap.newKeySet();
    // words
    static Set<String> banned_words = ConcurrentHashMap.newKeySet();
    static ConcurrentHashMap<String, AtomicInteger> words = new ConcurrentHashMap<>();
    static Set<String> wordsSet = ConcurrentHashMap.newKeySet();
    // per-thread most recent article
    static Article[] most_recent_articles = null;
    // queue of output-writing tasks
    static ArrayBlockingQueue<String> tasks = new ArrayBlockingQueue<>(100);
    // all unique articles, sorted later for all_articles.txt
    static List<Article> articles = Collections.synchronizedList(new ArrayList<>());

    // "best" values computed after processing
    static String best_category = null;
    static String best_word = null;
    static String best_language = null;

    // authors
    static Set<String> authorSet = ConcurrentHashMap.newKeySet();
    static ConcurrentHashMap<String, AtomicInteger> authors = new ConcurrentHashMap<>();
    static String best_author = null;


    public static void main(String[] args) throws IOException, InterruptedException {
        ObjectMapper mapper = new ObjectMapper();
        // the empty string is ignored when counting words
        banned_words.add("");
        // the most expensive writing tasks go first in the queue
        tasks.add("keywords_count.txt");
        tasks.add("all_articles.txt");

        // uuid -> article and title -> uuid, used for duplicate detection
        ConcurrentHashMap<String, Article> uuidToArticles = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, String> uuidToTitle = new ConcurrentHashMap<>();
        // thread-safe list of every uuid encountered
        List<String> articles_uuid = Collections.synchronizedList(new ArrayList<>());
        // sets of duplicates for fast lookup
        Set<String> duplicate_uuid = ConcurrentHashMap.newKeySet();
        Set<String> duplicate_title = ConcurrentHashMap.newKeySet();
        // parsed JSON files, split across threads
        List<ArrayNode> arrayNodeList = new ArrayList<>();
        if (args.length < 3) {
            System.err.println("Usage: java NewsOrganizer <num_threads> <articles.txt> <inputs.txt>");
            return;
        }
        int number_of_threads = Integer.parseInt(args[0]);
        // structures sized by the number of threads
        total = new int[number_of_threads];
        cnt = new int[number_of_threads];
        Thread[] threads = new Thread[number_of_threads];
        barrier = new CyclicBarrier(number_of_threads);
        most_recent_articles = new Article[number_of_threads];
        // read the list of article files
        File articles_file = new File(args[1]);
        Scanner scanner = new Scanner(articles_file);
        // paths are relative to the articles file
        String path = articles_file.getParent() + "/";
        while (scanner.hasNextLine()) {
            // number of article files
            String line = scanner.nextLine();
            int n = Integer.parseInt(line);
            for(int i=0; i<n; ++i){
                if(scanner.hasNextLine()) {
                    // parse a JSON file of articles
                    String string = scanner.nextLine();
                    File json_file = new File(path + string);
                    if(json_file.exists()) {
                        ArrayNode arrayNode = (ArrayNode) mapper.readTree(json_file);
                        arrayNodeList.add(arrayNode);
                    }
                }
            }
        }
        scanner.close();
        // read the auxiliary files
        File helper = new File(args[2]);
        scanner = new Scanner(helper);
        while(scanner.hasNextLine()) {
            String line = scanner.nextLine();
            line = scanner.nextLine();
            File languages_file = new File(path + line);
            line = scanner.nextLine();

            File categories_file = new File(path + line);
            line = scanner.nextLine();
            File word_file = new File(path + line);
            scanner.close();
            // languages: one writing task per language
            scanner = new Scanner(languages_file);
            while(scanner.hasNextLine()) {
                int n = Integer.parseInt(scanner.nextLine());
                for(int i=0; i<n; ++i){
                    line = scanner.nextLine();
                    tasks.add(line);
                    languagesSet.add(line);
                    languages.putIfAbsent(line, Collections.synchronizedList(new ArrayList<>()));
                }
            }
            // categories: same as languages
            scanner.close();
            scanner = new Scanner(categories_file);
            while(scanner.hasNextLine()) {
                int n = Integer.parseInt(scanner.nextLine());
                for(int i=0; i<n; ++i){
                    line = scanner.nextLine();
                    tasks.add(line);
                    categoriesSet.add(line);
                    categories.putIfAbsent(line, Collections.synchronizedList(new ArrayList<>()));
                }
            }
            scanner.close();
            // linking words are excluded from keyword counting
            scanner = new Scanner(word_file);
            while(scanner.hasNextLine()) {
                int n = Integer.parseInt(scanner.nextLine());
                for(int i=0; i<n; ++i){
                    banned_words.add(scanner.nextLine());
                }
            }
        }
        // start the workers
        for(int i=0; i<number_of_threads; ++i){
            threads[i] = new Worker(i, number_of_threads, uuidToArticles, uuidToTitle, duplicate_uuid, duplicate_title, articles_uuid, arrayNodeList);
            threads[i].start();
        }
        for (int i = 0; i < number_of_threads; ++i) {
            threads[i].join();
        }

    }
}
