import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MyThread extends Thread {
    // variabile pt thread
    private int thread_id;
    private int number_of_threads;

    // la astea nu am folosit Tema1. pentru ca mi-am amintit mai
    // tarziu ca puteam sa le fac statice si deja aveam constructor
    private ConcurrentHashMap<String, Article> uuid_Articles;
    private ConcurrentHashMap<String, String> title_uuid;
    private Set<String> duplicate_uuid;
    private Set<String> duplicate_title;
    private List<String> articles_uuid;
    private List<ArrayNode> arrayNodeList;
    // constr
    public MyThread(int thread_id, int number_of_threads,
                    ConcurrentHashMap<String, Article> uuid_Articles,
                    ConcurrentHashMap<String, String> title_uuid,
                    Set<String> duplicate_uuid,
                    Set<String> duplicate_title,
                    List<String> articles_uuid,
                    List<ArrayNode> arrayNodeList) {
        this.thread_id = thread_id;
        this.number_of_threads = number_of_threads;
        this.uuid_Articles = uuid_Articles;
        this.title_uuid = title_uuid;
        this.duplicate_uuid = duplicate_uuid;
        this.duplicate_title = duplicate_title;
        this.articles_uuid = articles_uuid;
        this.arrayNodeList = arrayNodeList;
    }

    @Override
    public void run() {
        // eliminarea duplicatelor
        int n = arrayNodeList.size();
        // impartire clasica pe threaduri
        int chunk = n / number_of_threads;
        int start = thread_id * chunk;
        int end = (thread_id == number_of_threads - 1) ? n : (thread_id + 1) * chunk;

        for (int i = start; i < end; ++i) {
            // parcurgem fiecare arraynode
            for (JsonNode jsonNode : arrayNodeList.get(i)) {
                String uuid = jsonNode.get("uuid").asText();
                String title = jsonNode.get("title").asText();
                String author = jsonNode.get("author").asText();
                String url = jsonNode.get("url").asText();
                String published = jsonNode.get("published").asText();
                String language = jsonNode.get("language").asText();
                String text = jsonNode.get("text").asText();
                JsonNode categories = jsonNode.get("categories");
                Set<String> catSet = new HashSet<>();
                if (categories.isArray()) {
                    for (JsonNode cat : categories) {
                        catSet.add(cat.asText());
                    }
                }
                // adaugam la articole procesate
                Tema1.total[thread_id]++;
                Article article = new Article(uuid, title, author, url, text, published, language, catSet);

                // lista cu toate uuid
                articles_uuid.add(uuid);

                // verificare duplicate
                Article existing = uuid_Articles.putIfAbsent(uuid, article);
                if (existing != null) {
                    duplicate_uuid.add(uuid);
                }

                String existingUuidForTitle = title_uuid.putIfAbsent(title, uuid);
                if (existingUuidForTitle != null) {
                    duplicate_title.add(title);
                }
            }
        }
        try {
            Tema1.barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
        // etapa 2 procesare

        // folosim hashmap local pentru ca e mai rapid pentru ca sunt mai putine conflicte pe threaduri
        // am incercat as fac si cu categories si cu languages dar mergea mai prost
        Map<String, Integer> localWordCounts = new HashMap<>();
        Map<String, Integer> localAuthorCounts = new HashMap<>();

        // impartire clasica din nou
        int totalArticles = articles_uuid.size();
        int chunk2 = totalArticles / number_of_threads;
        int start2 = thread_id * chunk2;
        int end2 = (thread_id == number_of_threads - 1) ? totalArticles : (thread_id + 1) * chunk2;
        for (int i = start2; i < end2; ++i) {
            String uuid = articles_uuid.get(i);
            if (duplicate_uuid.contains(uuid)) continue;
            Article article = uuid_Articles.get(uuid);
            if (article == null || duplicate_title.contains(article.getTitle())) continue;
            // nu e duplicat
            Tema1.cnt[thread_id]++; // counter
            Tema1.articles.add(article); // lista de articole

            // set de autori si map
            Tema1.authorSet.add(article.getAuthor());
            localAuthorCounts.put(article.getAuthor(), localAuthorCounts.getOrDefault(article.getAuthor(), 0) + 1);

            // cel mai recent partial
            if (Tema1.most_recent_articles[thread_id] == null) {
                Tema1.most_recent_articles[thread_id] = article;
            } else {
                if (article.compareTo(Tema1.most_recent_articles[thread_id]) < 0) {
                    Tema1.most_recent_articles[thread_id] = article;
                }
            }
            // pentru limbi
            String language = article.getLanguage();
            if(Tema1.languagesSet.contains(language)){
                Tema1.languages.get(language).add(article.getUuid());
            }
            // pentru categorii prima data verificam daca are toate articolele valide
            boolean ok = true;
            for(String category : article.getCategory()){
                if(!Tema1.categoriesSet.contains(category)){
                    ok = false;
                    break;
                }
            }
            if(ok){
                for(String category : article.getCategory()){
                    Tema1.categories.get(category).add(article.getUuid());
                }
            }
            // cuvinteele
            if(language.equals("english")){
                // luam cuvintele ca in enunt
                String[] text_words = article.getText().toLowerCase().split("\\s+");
                // set pentru a nu avea dubluri
                Set<String> uniqueWordsInArticle = new HashSet<>();
                for(String rawWord : text_words){
                    uniqueWordsInArticle.add(rawWord.replaceAll("[^a-z]+", ""));
                }
                for(String word : uniqueWordsInArticle){
                    if(Tema1.banned_words.contains(word)) continue; // cuvintele interzise
                    Tema1.wordsSet.add(word);
                    localWordCounts.put(word, localWordCounts.getOrDefault(word, 0) + 1);
                }
            }
        }
        // combinam autorii
        for (Map.Entry<String, Integer> entry : localAuthorCounts.entrySet()) {
            Tema1.authors.computeIfAbsent(entry.getKey(), k -> new AtomicInteger(0)).addAndGet(entry.getValue());
        }

        // Vărsăm cuvintele
        for (Map.Entry<String, Integer> entry : localWordCounts.entrySet()) {
            Tema1.words.computeIfAbsent(entry.getKey(), k-> new AtomicInteger(0)).addAndGet(entry.getValue());
        }
        // bariera ca aici stim ca avem toate articolele procesate
        try {
            Tema1.barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }

        int k = 4;
        int s = (int) (thread_id * (double) k / number_of_threads);
        int e = (int) Math.min(k, (thread_id + 1) * (double) k / number_of_threads);

        // total si cel mai recent
        if (s <= 0 && 0 < e) {
            Article globalBest = null;
            for (int i = 0; i < number_of_threads; ++i) {
                if(i != 0){
                    Tema1.total[0] += Tema1.total[i];
                    Tema1.cnt[0] += Tema1.cnt[i];
                }
                Article current = Tema1.most_recent_articles[i];
                if (current == null) continue;
                if (globalBest == null || current.compareTo(globalBest) < 0) {
                    globalBest = current;
                }
            }
            Tema1.most_recent_articles[0] = globalBest;
        }
        // top categorie
        if (s <= 1 && 1 < e) {
            String best = null;
            for (String category : Tema1.categoriesSet) {
                if (best == null) { best = category; } else {
                    int sizeBest = Tema1.categories.get(best).size();
                    int sizeCurr = Tema1.categories.get(category).size();
                    if (sizeCurr > sizeBest || (sizeCurr == sizeBest && best.compareTo(category) > 0)) {
                        best = category;
                    }
                }
            }
            Tema1.best_category = best;
        }

        // top limba
        if (s <= 2 && 2 < e) {
            String best = null;
            for (String language : Tema1.languagesSet) {
                if (best == null) { best = language; } else {
                    int sizeBest = Tema1.languages.get(best).size();
                    int sizeCurr = Tema1.languages.get(language).size();
                    if (sizeCurr > sizeBest || (sizeCurr == sizeBest && best.compareTo(language) > 0)) {
                        best = language;
                    }
                }
            }
            Tema1.best_language = best;
        }

        // top autori
        if (s <= 3 && 3 < e) {
            String best = null;
            for (String author : Tema1.authorSet) {
                if (best == null) {
                    best = author;
                } else {
                    int countBest = Tema1.authors.get(best).get();
                    int countCurr = Tema1.authors.get(author).get();
                    if (countCurr > countBest || (countCurr == countBest && best.compareTo(author) > 0)) {
                        best = author;
                    }
                }
            }
            Tema1.best_author = best;
        }
        // nu avem nevoie de bariera pentru ca singurul care depinde de ce e mai sus este cel cu reports
        // care se face imediat dupa keywords care dureaza mai mult
        // etapa finala scrierea
        while(true){
            String task = Tema1.tasks.poll();
            if (task == null) break;
            process(task);
            if(task.equals("keywords_count.txt")){
                process("reports.txt");
            }
        }
    }

    // functie pentru scriere
    public void process(String task) {
        if(task.equals("all_articles.txt")){
            Collections.sort(Tema1.articles);
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(task))) {
                for(Article article : Tema1.articles) bw.write(article.toString() + "\n");
            } catch (IOException ex) { throw new RuntimeException(ex); }
        }
        else if(task.equals("keywords_count.txt")){
            List<String> sortedKeywords = new ArrayList<>(Tema1.wordsSet);
            Collections.sort(sortedKeywords, (w1, w2) -> {
                int count1 = Tema1.words.get(w1).get();
                int count2 = Tema1.words.get(w2).get();
                if (count1 != count2) return Integer.compare(count2, count1);
                return w1.compareTo(w2);
            });
            System.out.println("da");
            Tema1.best_word = sortedKeywords.get(0);
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(task))) {
                for(String word : sortedKeywords) bw.write(word + " " + Tema1.words.get(word).get() + "\n");
            } catch (IOException ex) { throw new RuntimeException(ex); }
        }
        else if(task.equals("reports.txt")){
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(task))) {
                // Nota: Tema1.total[0] si Tema1.cnt[0] au fost agregate mai sus la Task 0
                bw.write("duplicates_found - " + (Tema1.total[0] - Tema1.cnt[0]) + "\n");
                bw.write("unique_articles - " + (Tema1.cnt[0])+ "\n");
                bw.write("best_author - " + Tema1.best_author + " " + Tema1.authors.get(Tema1.best_author).get()+ "\n");
                bw.write("top_language - " + Tema1.best_language + " " +  Tema1.languages.get(Tema1.best_language).size() + "\n");
                bw.write("top_category - " + Tema1.best_category.replaceAll("[ ,]+", "_") + " " +  Tema1.categories.get(Tema1.best_category).size() + "\n");
                bw.write("most_recent_article - " + Tema1.most_recent_articles[0].getPublished() + " " +  Tema1.most_recent_articles[0].getUrl() + "\n");
                bw.write("top_keyword_en - " + Tema1.best_word + " " +  Tema1.words.get(Tema1.best_word).get() + "\n");
            } catch (IOException ex) { throw new RuntimeException(ex); }
        }
        else if(Tema1.languagesSet.contains(task)){
            try {
                if(!Tema1.languages.get(task).isEmpty()) {
                    List<String> sorted = Tema1.languages.get(task);
                    Collections.sort(sorted);
                    try(BufferedWriter bw = new BufferedWriter(new FileWriter(task + ".txt"))){
                        for(String s2 : sorted) bw.write(s2 + "\n");
                    }
                }
            } catch (IOException ex) { throw new RuntimeException(ex); }
        }
        else { // Categories
            String path = task.replaceAll("[^a-zA-Z]+", "_") + ".txt";
            try {
                if(!Tema1.categories.get(task).isEmpty()) {
                    List<String> sorted = Tema1.categories.get(task);
                    Collections.sort(sorted);
                    try(BufferedWriter bw = new BufferedWriter(new FileWriter(path))){
                        for(String s2 : sorted) bw.write(s2 + "\n");
                    }
                }
            } catch (IOException ex) { throw new RuntimeException(ex); }
        }
    }
}
