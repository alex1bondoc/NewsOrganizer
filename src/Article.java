import java.util.Set;


// clasa pentru a retine date despre articole
// comparable pentru a putea folosi Collections.sort
public class Article implements Comparable<Article> {
    private String uuid;
    private String title;
    private String author;
    private String url;
    private String text;
    private String published;
    private String language;
    private Set<String> category;

    public Article(String uuid, String title, String author, String url,
                   String text, String published, String language, Set<String> category) {
        this.uuid = uuid;
        this.title = title;
        this.author = author;
        this.url = url;
        this.text = text;
        this.published = published;
        this.language = language;
        this.category = category;
    }

    public String getUuid() { return uuid; }
    public String getTitle() { return title; }
    public String getPublished() { return published; }
    public String getUrl() { return url; } // Ai nevoie de URL pentru output
    public String getText() { return text; }
    public String getLanguage() { return language; }
    public Set<String> getCategory() { return category; }
    public String getAuthor() { return author; }

    @Override
    public String toString() {
        return uuid + " " + published;
    }

    @Override
    public int compareTo(Article other) {
        // comparam data de publicare
        int dateComp = other.getPublished().compareTo(this.published);
        if (dateComp != 0) {
            return dateComp;
        }
        // daca sunt egale datele comparam uuid
        return this.uuid.compareTo(other.getUuid());
    }
}