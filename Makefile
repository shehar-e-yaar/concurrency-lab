CC = gcc
CFLAGS = -std=c11 -O2 -Wall -Wextra -Wpedantic -pthread

lab: lab.c
	$(CC) $(CFLAGS) lab.c -o lab

test: lab
	python3 verify.py

tsan:
	$(CC) -std=c11 -g -O1 -Wall -Wextra -pthread -fsanitize=thread lab.c -o lab-tsan
	./lab-tsan pool 8 500 0 /tmp/concurrency-lab-tsan.jsonl
	./lab-tsan race
	./lab-tsan deadlock
	./lab-tsan wait

clean:
	rm -f lab lab-tsan events.jsonl benchmark.csv

.PHONY: test tsan clean
