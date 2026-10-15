# NewsOrganizer

Parallel processing of a large set of news articles (JSON) in Java: duplicate removal, grouping by category and language, keyword counting and generation of a final report.

## Project structure

```
src/
├── Tema1.java       # entry point: reads the input and starts the threads
├── MyThread.java    # logic run by each thread (the 4 stages)
├── Article.java     # article model
├── Makefile
├── libs/            # Jackson (core, databind, annotations) for JSON parsing
├── input/           # sample input files
└── output/          # sample generated files
```

## Build and run

Requires Java 17+. The Jackson libraries are included in `src/libs`.

```bash
cd src
make build
make run ARGS="<num_threads> <articles.txt> <inputs.txt>"
```

Example:

```bash
make run ARGS="4 input/articles.txt input/inputs.txt"
```

- `articles.txt` – the first line holds the number of files, followed by the paths to the JSON article files.
- `inputs.txt` – the first line holds the number of auxiliary files, followed by the paths to the language list, the category list and the list of linking words (which are ignored when counting words).

`make clean` removes the generated `.class` and `.txt` files.

## Output files

- one file per category (e.g. `Politics.txt`, `Sport.txt`) and per language (e.g. `english.txt`, `romanian.txt`)
- `all_articles.txt` – all unique articles
- `keywords_count.txt` – keyword frequencies
- `reports.txt` – statistics: number of articles, duplicates, authors, most popular category, language and keyword, most recent article

## Parallelization strategy

Execution runs in four stages, each separated by a barrier (`CyclicBarrier`), so no thread moves to the next stage until every thread has finished the current one.

1. **Reading and duplicate detection.** The input files are read in `main`, and the articles are stored as `ArrayNode`s in a list. The list is split evenly across threads using the classic formula `start = id * (n / P)`, `end = (id + 1) * (n / P)`. Each thread records the UUIDs it encounters in concurrent hash maps and sets to identify duplicate articles.
2. **Article processing.** Threads iterate over the UUIDs, skip duplicates and process the rest. Authors, categories, languages and words are tracked in `ConcurrentHashMap`s (with `AtomicInteger` counters) and in concurrent sets.
3. **Report precomputation.** The four main parts of `reports.txt` (article counts, authors, most popular category, most popular language) are split evenly across threads. The most recent article and the total article count are computed from per-thread partial results.
4. **Writing the output.** Write tasks are placed in an `ArrayBlockingQueue`, from which threads pull work dynamically, since some files take much longer than others. The first task is `keywords_count.txt` (always the most expensive), followed immediately by `reports.txt`, which uses the sorted keywords to find the most popular word.

### Synchronized structures used

| Structure | Used for |
|---|---|
| `ConcurrentHashMap` | authors, languages, categories, words |
| `Collections.synchronizedList` | articles |
| concurrent sets | languages, categories, words, authors |
| `AtomicInteger` | occurrence counts for authors and words |
| `ArrayBlockingQueue` | distributing the write tasks |
| `CyclicBarrier` | synchronization between stages |

The solution aims to balance work as evenly as possible, especially during writing, where `all_articles.txt` and `keywords_count.txt` are much larger than the others and must run in parallel with the rest. It is thread-safe and produces consistent results on the same input.

## Performance analysis

Test environment: Intel Core i7-12700H (2.30 GHz, 14 cores), WSL Ubuntu, Java 17.0.17. Dataset: the largest available test set.

Execution times (seconds):

| Run | 1 thread | 2 | 3 | 4 | 6 | 8 |
|---|---|---|---|---|---|---|
| R1 | 13.215 | 8.306 | 6.618 | 7.044 | 6.621 | 5.285 |
| R2 | 13.604 | 8.628 | 8.312 | 6.961 | 6.039 | 5.477 |
| R3 | 13.866 | 9.953 | 7.495 | 5.894 | 6.661 | 5.781 |
| **Average** | **13.561** | **8.962** | **7.475** | **6.633** | **6.440** | **5.514** |

Speedup and efficiency (from average times):

| Threads | 1 | 2 | 3 | 4 | 6 | 8 |
|---|---|---|---|---|---|---|
| Speedup S(P) | 1.000 | 1.513 | 1.814 | 2.044 | 2.105 | 2.459 |
| Efficiency E(P) | 1.000 | 0.756 | 0.605 | 0.511 | 0.351 | 0.307 |

### Observed behavior

- **1–4 threads:** a sharp drop in execution time, from about 13.5 s to about 6.6 s. In this range, adding cores directly speeds up the work.
- **4–8 threads:** the curve flattens. 8 threads give the lowest time (under 6 s) and the highest speedup (~2.46x), but the gain over 4 threads is small compared to the initial jump.

### Causes of the limitations

The maximum speedup of ~2.46x, far from 8x, comes from:

- **Synchronization overhead.** Access to shared structures (`ConcurrentHashMap`, synchronized lists) and waiting at the `CyclicBarrier` take time; with more threads, contention on the same resources grows.
- **Dataset size.** Processing a single article is very fast, so coordination cost becomes comparable to the actual work.
- **Large output files.** Total time depends heavily on writing `keywords_count.txt` and `all_articles.txt`; other workers may run out of tasks while these finish.

### Optimal thread count

**4 threads** give the best balance between resources used and speed. Although 8 threads are slightly faster, per-thread efficiency drops significantly beyond 4.
