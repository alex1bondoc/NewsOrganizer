THREADS ?= 4
ARGS ?= $(THREADS) input/articles.txt input/inputs.txt

build:
	javac -cp ".:libs/*" *.java

run: build
	java -cp ".:libs/*" NewsOrganizer $(ARGS)

clean:
	rm -f *.class *.txt

.PHONY: build run clean
