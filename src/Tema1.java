import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class Tema1 {
    // partiale pentru valorile din reports.txt
    static int[] total = null;
    static int[] cnt = null;
    // bariera pentru sincronizare
    static CyclicBarrier barrier;
    // categorii
    static ConcurrentHashMap<String, List<String>> categories = new ConcurrentHashMap<>();
    static Set<String> categoriesSet = ConcurrentHashMap.newKeySet();
    //limbi
    static ConcurrentHashMap<String, List<String>> languages = new ConcurrentHashMap<>();
    static Set<String> languagesSet = ConcurrentHashMap.newKeySet();
    // cuvinte
    static Set<String> banned_words = ConcurrentHashMap.newKeySet();
    static ConcurrentHashMap<String, AtomicInteger> words = new ConcurrentHashMap<>();
    static Set<String> wordsSet = ConcurrentHashMap.newKeySet();
    // partial pentru cel mai recent articol
    static Article[] most_recent_articles = null;
    // queue pentru taskurile de scriere
    static ArrayBlockingQueue<String> tasks = new ArrayBlockingQueue<>(100);
    // lista de articole cu toate articolele valide pentru a le sorta in all_articles.txt
    static List<Article> articles = Collections.synchronizedList(new ArrayList<>());

    // valori best pe care le calculam dupa procesare
    static String best_category = null;
    static String best_word = null;
    static String best_language = null;

    // pentru autori
    static Set<String> authorSet = ConcurrentHashMap.newKeySet();
    static ConcurrentHashMap<String, AtomicInteger> authors = new ConcurrentHashMap<>();
    static String best_author = null;


    public static void main(String[] args) throws IOException, InterruptedException {
        ObjectMapper mapper = new ObjectMapper();
        // adaugam "" in banned words pentru ca daca nu imi pica programul
        banned_words.add("");
        // adaugam cele mai grele taskuri in queue ul de afisare
        tasks.add("keywords_count.txt");
        tasks.add("all_articles.txt");

        // map de la uuid la articol pentru thread si title uuid
        ConcurrentHashMap<String, Article> uuidToArticles = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, String> uuidToTitle = new ConcurrentHashMap<>();
        // lista de uuid threadsafe
        List<String> articles_uuid = Collections.synchronizedList(new ArrayList<>());
        // seturi pentru duplicate pentru verificare rapida
        Set<String> duplicate_uuid = ConcurrentHashMap.newKeySet();
        Set<String> duplicate_title = ConcurrentHashMap.newKeySet();
        // lista pentru taskurile de citire
        List<ArrayNode> arrayNodeList = new ArrayList<>();
        // rulare corecta
        if (args.length < 2) {
            return;
        }
        // primul argument
        int number_of_threads = Integer.parseInt(args[0]);
        // initializam ceea ce depinde de marimea primului arg
        total = new int[number_of_threads];
        cnt = new int[number_of_threads];
        Thread[] threads = new Thread[number_of_threads];
        barrier = new CyclicBarrier(number_of_threads);
        most_recent_articles = new Article[number_of_threads];
        // incepem citirea
        File articles_file = new File(args[1]);
        Scanner scanner = new Scanner(articles_file);
        // fisier parinte
        String path = articles_file.getParent() + "/";
        while (scanner.hasNextLine()) {
            // numarul de articole
            String line = scanner.nextLine();
            int n = Integer.parseInt(line);
            for(int i=0; i<n; ++i){
                if(scanner.hasNextLine()) {
                    // citim arraynode ul de articole din fisier json
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
        // citim celalalt fisier
        File helper = new File(args[2]);
        scanner = new Scanner(helper);
        while(scanner.hasNextLine()) {
            // citim fisierele din interior
            String line = scanner.nextLine();
            line = scanner.nextLine();
            File languages_file = new File(path + line);
            line = scanner.nextLine();

            File categories_file = new File(path + line);
            line = scanner.nextLine();
            File word_file = new File(path + line);
            scanner.close();
            // citim limbile si creem taskuri de scriere pentru ele
            scanner = new Scanner(languages_file);
            while(scanner.hasNextLine()) {
                int n = Integer.parseInt(scanner.nextLine());
                for(int i=0; i<n; ++i){
                    line = scanner.nextLine();
                    tasks.add(line);
                    // initializam setul si mapul
                    languagesSet.add(line);
                    languages.putIfAbsent(line, Collections.synchronizedList(new ArrayList<>()));
                }
            }
            // pentru categorii exact ca la limbi
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
            // la cuvinte adaugam doar in lista de cuvinte pe care nu le luam in calcul
            scanner = new Scanner(word_file);
            while(scanner.hasNextLine()) {
                int n = Integer.parseInt(scanner.nextLine());
                for(int i=0; i<n; ++i){
                    banned_words.add(scanner.nextLine());
                }
            }
        }
        // pornim exact numarul de threaduri
        for(int i=0; i<number_of_threads; ++i){
            // Pasăm lista și map-ul simplu
            threads[i] = new MyThread(i, number_of_threads, uuidToArticles, uuidToTitle, duplicate_uuid, duplicate_title, articles_uuid, arrayNodeList);
            threads[i].start();
        }
        // dam join
        for (int i = 0; i < number_of_threads; ++i) {
            threads[i].join();
        }

    }
}