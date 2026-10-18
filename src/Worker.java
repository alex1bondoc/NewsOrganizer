import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Worker extends Thread {
    // thread info
    private int thread_id;
    private int number_of_threads;

    // shared structures passed in from main
    private ConcurrentHashMap<String, Article> uuid_Articles;
    private ConcurrentHashMap<String, String> title_uuid;
    private Set<String> duplicate_uuid;
    private Set<String> duplicate_title;
    private List<String> articles_uuid;
    private List<ArrayNode> arrayNodeList;
    public Worker(int thread_id, int number_of_threads,
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
        // stage 1: duplicate detection
        int n = arrayNodeList.size();
        // static split of the input files across threads
        int chunk = n / number_of_threads;
        int start = thread_id * chunk;
        int end = (thread_id == number_of_threads - 1) ? n : (thread_id + 1) * chunk;

        for (int i = start; i < end; ++i) {
            // iterate over every article in the file
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
                // count every article read
                NewsOrganizer.total[thread_id]++;
                Article article = new Article(uuid, title, author, url, text, published, language, catSet);

                // list of all uuids
                articles_uuid.add(uuid);

                // duplicate check by uuid and by title
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
            NewsOrganizer.barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
        // stage 2: article processing

        // thread-local maps avoid contention; they are merged into the shared maps at the end
        // (tried the same for categories and languages, but it was slower)
        Map<String, Integer> localWordCounts = new HashMap<>();
        Map<String, Integer> localAuthorCounts = new HashMap<>();

        // static split of the uuids across threads
        int totalArticles = articles_uuid.size();
        int chunk2 = totalArticles / number_of_threads;
        int start2 = thread_id * chunk2;
        int end2 = (thread_id == number_of_threads - 1) ? totalArticles : (thread_id + 1) * chunk2;
        for (int i = start2; i < end2; ++i) {
            String uuid = articles_uuid.get(i);
            if (duplicate_uuid.contains(uuid)) continue;
            Article article = uuid_Articles.get(uuid);
            if (article == null || duplicate_title.contains(article.getTitle())) continue;
            // not a duplicate
            NewsOrganizer.cnt[thread_id]++;
            NewsOrganizer.articles.add(article);

            // authors
            NewsOrganizer.authorSet.add(article.getAuthor());
            localAuthorCounts.put(article.getAuthor(), localAuthorCounts.getOrDefault(article.getAuthor(), 0) + 1);

            // per-thread most recent article
            if (NewsOrganizer.most_recent_articles[thread_id] == null) {
                NewsOrganizer.most_recent_articles[thread_id] = article;
            } else {
                if (article.compareTo(NewsOrganizer.most_recent_articles[thread_id]) < 0) {
                    NewsOrganizer.most_recent_articles[thread_id] = article;
                }
            }
            // languages
            String language = article.getLanguage();
            if(NewsOrganizer.languagesSet.contains(language)){
                NewsOrganizer.languages.get(language).add(article.getUuid());
            }
            // categories: only if all of the article's categories are known
            boolean ok = true;
            for(String category : article.getCategory()){
                if(!NewsOrganizer.categoriesSet.contains(category)){
                    ok = false;
                    break;
                }
            }
            if(ok){
                for(String category : article.getCategory()){
                    NewsOrganizer.categories.get(category).add(article.getUuid());
                }
            }
            // keywords (English articles only)
            if(language.equals("english")){
                // tokenize on whitespace and keep only letters
                String[] text_words = article.getText().toLowerCase().split("\\s+");
                // count each word at most once per article
                Set<String> uniqueWordsInArticle = new HashSet<>();
                for(String rawWord : text_words){
                    uniqueWordsInArticle.add(rawWord.replaceAll("[^a-z]+", ""));
                }
                for(String word : uniqueWordsInArticle){
                    if(NewsOrganizer.banned_words.contains(word)) continue; // linking words
                    NewsOrganizer.wordsSet.add(word);
                    localWordCounts.put(word, localWordCounts.getOrDefault(word, 0) + 1);
                }
            }
        }
        // merge local author counts
        for (Map.Entry<String, Integer> entry : localAuthorCounts.entrySet()) {
            NewsOrganizer.authors.computeIfAbsent(entry.getKey(), k -> new AtomicInteger(0)).addAndGet(entry.getValue());
        }

        // merge local word counts
        for (Map.Entry<String, Integer> entry : localWordCounts.entrySet()) {
            NewsOrganizer.words.computeIfAbsent(entry.getKey(), k-> new AtomicInteger(0)).addAndGet(entry.getValue());
        }
        // after this barrier every article has been processed
        try {
            NewsOrganizer.barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }

        int k = 4;
        int s = (int) (thread_id * (double) k / number_of_threads);
        int e = (int) Math.min(k, (thread_id + 1) * (double) k / number_of_threads);

        // stage 3: report precomputation, 4 tasks split across threads
        // total counts and most recent article
        if (s <= 0 && 0 < e) {
            Article globalBest = null;
            for (int i = 0; i < number_of_threads; ++i) {
                if(i != 0){
                    NewsOrganizer.total[0] += NewsOrganizer.total[i];
                    NewsOrganizer.cnt[0] += NewsOrganizer.cnt[i];
                }
                Article current = NewsOrganizer.most_recent_articles[i];
                if (current == null) continue;
                if (globalBest == null || current.compareTo(globalBest) < 0) {
                    globalBest = current;
                }
            }
            NewsOrganizer.most_recent_articles[0] = globalBest;
        }
        // top category
        if (s <= 1 && 1 < e) {
            String best = null;
            for (String category : NewsOrganizer.categoriesSet) {
                if (best == null) { best = category; } else {
                    int sizeBest = NewsOrganizer.categories.get(best).size();
                    int sizeCurr = NewsOrganizer.categories.get(category).size();
                    if (sizeCurr > sizeBest || (sizeCurr == sizeBest && best.compareTo(category) > 0)) {
                        best = category;
                    }
                }
            }
            NewsOrganizer.best_category = best;
        }

        // top language
        if (s <= 2 && 2 < e) {
            String best = null;
            for (String language : NewsOrganizer.languagesSet) {
                if (best == null) { best = language; } else {
                    int sizeBest = NewsOrganizer.languages.get(best).size();
                    int sizeCurr = NewsOrganizer.languages.get(language).size();
                    if (sizeCurr > sizeBest || (sizeCurr == sizeBest && best.compareTo(language) > 0)) {
                        best = language;
                    }
                }
            }
            NewsOrganizer.best_language = best;
        }

        // top author
        if (s <= 3 && 3 < e) {
            String best = null;
            for (String author : NewsOrganizer.authorSet) {
                if (best == null) {
                    best = author;
                } else {
                    int countBest = NewsOrganizer.authors.get(best).get();
                    int countCurr = NewsOrganizer.authors.get(author).get();
                    if (countCurr > countBest || (countCurr == countBest && best.compareTo(author) > 0)) {
                        best = author;
                    }
                }
            }
            NewsOrganizer.best_author = best;
        }
        // stage 4: writing the output, tasks are pulled dynamically from the queue
        // no barrier needed: only reports.txt depends on stage 3, and it runs right after
        // keywords_count.txt, which is always the slowest task
        while(true){
            String task = NewsOrganizer.tasks.poll();
            if (task == null) break;
            process(task);
            if(task.equals("keywords_count.txt")){
                process("reports.txt");
            }
        }
    }

    // writes one output file
    public void process(String task) {
        if(task.equals("all_articles.txt")){
            Collections.sort(NewsOrganizer.articles);
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(task))) {
                for(Article article : NewsOrganizer.articles) bw.write(article.toString() + "\n");
            } catch (IOException ex) { throw new RuntimeException(ex); }
        }
        else if(task.equals("keywords_count.txt")){
            List<String> sortedKeywords = new ArrayList<>(NewsOrganizer.wordsSet);
            Collections.sort(sortedKeywords, (w1, w2) -> {
                int count1 = NewsOrganizer.words.get(w1).get();
                int count2 = NewsOrganizer.words.get(w2).get();
                if (count1 != count2) return Integer.compare(count2, count1);
                return w1.compareTo(w2);
            });
            NewsOrganizer.best_word = sortedKeywords.get(0);
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(task))) {
                for(String word : sortedKeywords) bw.write(word + " " + NewsOrganizer.words.get(word).get() + "\n");
            } catch (IOException ex) { throw new RuntimeException(ex); }
        }
        else if(task.equals("reports.txt")){
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(task))) {
                // total[0] and cnt[0] were aggregated in stage 3
                bw.write("duplicates_found - " + (NewsOrganizer.total[0] - NewsOrganizer.cnt[0]) + "\n");
                bw.write("unique_articles - " + (NewsOrganizer.cnt[0])+ "\n");
                bw.write("best_author - " + NewsOrganizer.best_author + " " + NewsOrganizer.authors.get(NewsOrganizer.best_author).get()+ "\n");
                bw.write("top_language - " + NewsOrganizer.best_language + " " +  NewsOrganizer.languages.get(NewsOrganizer.best_language).size() + "\n");
                bw.write("top_category - " + NewsOrganizer.best_category.replaceAll("[ ,]+", "_") + " " +  NewsOrganizer.categories.get(NewsOrganizer.best_category).size() + "\n");
                bw.write("most_recent_article - " + NewsOrganizer.most_recent_articles[0].getPublished() + " " +  NewsOrganizer.most_recent_articles[0].getUrl() + "\n");
                bw.write("top_keyword_en - " + NewsOrganizer.best_word + " " +  NewsOrganizer.words.get(NewsOrganizer.best_word).get() + "\n");
            } catch (IOException ex) { throw new RuntimeException(ex); }
        }
        else if(NewsOrganizer.languagesSet.contains(task)){
            try {
                if(!NewsOrganizer.languages.get(task).isEmpty()) {
                    List<String> sorted = NewsOrganizer.languages.get(task);
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
                if(!NewsOrganizer.categories.get(task).isEmpty()) {
                    List<String> sorted = NewsOrganizer.categories.get(task);
                    Collections.sort(sorted);
                    try(BufferedWriter bw = new BufferedWriter(new FileWriter(path))){
                        for(String s2 : sorted) bw.write(s2 + "\n");
                    }
                }
            } catch (IOException ex) { throw new RuntimeException(ex); }
        }
    }
}
