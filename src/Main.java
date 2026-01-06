//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ArrayNode;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.CyclicBarrier;
//import java.util.concurrent.SubmissionPublisher;
//import java.util.concurrent.atomic.AtomicInteger;
//
//public class Main {
//    static AtomicInteger counter = new AtomicInteger(0);
//    static int[] total = new int[4];
//    static int[] cnt = new int[4];
//    static AtomicInteger counter2 = new AtomicInteger(0);
//    static CyclicBarrier barrier;
//    static ConcurrentHashMap<String, List<String>> categories = new ConcurrentHashMap<>();
//    static Set<String> categoriesSet = ConcurrentHashMap.newKeySet();
//
//    static ConcurrentHashMap<String, List<String>> languages = new ConcurrentHashMap<>();
//    static Set<String> languagesSet = ConcurrentHashMap.newKeySet();
//
//    static Set<String> banned_words = ConcurrentHashMap.newKeySet();
//    static ConcurrentHashMap<String, AtomicInteger> words = new ConcurrentHashMap<>();
//    static Set<String> wordsSet = ConcurrentHashMap.newKeySet();
//    static Article[] most_recent_articles = null;
//    static List<String> tasks = new ArrayList<>();
//    static List<Article> articles = Collections.synchronizedList(new ArrayList<>());
//    static String best_category = null;
//    static String best_word = null;
//    static String best_language = null;
//
//    static Set<String> authorSet = ConcurrentHashMap.newKeySet();
//    static ConcurrentHashMap<String, AtomicInteger> authors = new ConcurrentHashMap<>();
//    static String best_author = null;
//
//
//    public static void main(String[] args) throws IOException, InterruptedException {
//        ObjectMapper mapper = new ObjectMapper();
//        tasks.add("all_articles.txt");
//        tasks.add("reports.txt");
//        banned_words.add("");
//        // 1. Așa cum ai cerut: Map de la String la Article (NU listă)
//        ConcurrentHashMap<String, Article> uuidToArticles = new ConcurrentHashMap<>();
//        ConcurrentHashMap<String, String> uuidToTitle = new ConcurrentHashMap<>();
//        total[0] = 0;
//        // 2. Așa cum ai cerut: Listă sincronizată pentru a ține toate UUID-urile (inclusiv duplicatele)
//        List<String> articles_uuid = Collections.synchronizedList(new ArrayList<>());
//
//        Set<String> duplicate_uuid = ConcurrentHashMap.newKeySet();
//        Set<String> duplicate_title = ConcurrentHashMap.newKeySet();
//
//        List<ArrayNode> arrayNodeList = new ArrayList<>();
//
//        if (args.length < 2) {
//            return;
//        }
//
//        int number_of_threads = Integer.parseInt(args[0]);
//        Thread[] threads = new Thread[number_of_threads];
//        barrier = new CyclicBarrier(number_of_threads);
//        most_recent_articles = new Article[number_of_threads];
//        File articles_file = new File(args[1]);
//        Scanner scanner = new Scanner(articles_file);
//        String path = articles_file.getParent() + "/";
//
//        while (scanner.hasNextLine()) {
//            String line = scanner.nextLine();
//            if(line.isEmpty()) continue;
//            int n = Integer.parseInt(line);
//            for(int i=0; i<n; ++i){
//                if(scanner.hasNextLine()) {
//                    String string = scanner.nextLine();
//                    File json_file = new File(path + string);
//                    if(json_file.exists()) {
//                        ArrayNode arrayNode = (ArrayNode) mapper.readTree(json_file);
//                        arrayNodeList.add(arrayNode);
//                    }
//                }
//            }
//        }
//        scanner.close();
//        File helper = new File(args[2]);
//        scanner = new Scanner(helper);
//        while(scanner.hasNextLine()) {
//            String line = scanner.nextLine();
//            line = scanner.nextLine();
//            File languages_file = new File(path + line);
//            line = scanner.nextLine();
//
//            File categories_file = new File(path + line);
//            line = scanner.nextLine();
//            File word_file = new File(path + line);
//            scanner.close();
//            scanner = new Scanner(languages_file);
//            while(scanner.hasNextLine()) {
//                int n = Integer.parseInt(scanner.nextLine());
//                for(int i=0; i<n; ++i){
//                    line = scanner.nextLine();
//                    tasks.add(line);
//                    languagesSet.add(line);
//                    languages.putIfAbsent(line, Collections.synchronizedList(new ArrayList<>()));
//                }
//            }
//            tasks.add("keywords_count.txt");
//
//            scanner.close();
//            scanner = new Scanner(categories_file);
//            while(scanner.hasNextLine()) {
//                int n = Integer.parseInt(scanner.nextLine());
//                for(int i=0; i<n; ++i){
//                    line = scanner.nextLine();
//                    tasks.add(line);
//                    categoriesSet.add(line);
//                    categories.putIfAbsent(line, Collections.synchronizedList(new ArrayList<>()));
//                }
//            }
//            scanner.close();
//            scanner = new Scanner(word_file);
//            while(scanner.hasNextLine()) {
//                int n = Integer.parseInt(scanner.nextLine());
//                for(int i=0; i<n; ++i){
//                    banned_words.add(scanner.nextLine());
//                }
//            }
//        }
//
//
//        for(int i=0; i<number_of_threads; ++i){
//            // Pasăm lista și map-ul simplu
//            threads[i] = new MyThread(i, number_of_threads, uuidToArticles, uuidToTitle, duplicate_uuid, duplicate_title, articles_uuid, arrayNodeList);
//            threads[i].start();
//        }
//
//        for (int i = 0; i < number_of_threads; ++i) {
//            threads[i].join();
//        }
//
//        // Verificare
//    }
//}