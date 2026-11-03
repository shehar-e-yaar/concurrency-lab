#define _POSIX_C_SOURCE 200809L
#include <pthread.h>
#include <semaphore.h>
#include <stdatomic.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define MAX_WORKERS 8
#define CAPACITY 16
static void check(int e) { if (e) { fprintf(stderr, "%s\n", strerror(e)); exit(1); } }
static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec / 1e9; }
static void pause_ms(int ms) { struct timespec t = {ms / 1000, (ms % 1000) * 1000000L}; while (nanosleep(&t, &t) && errno == EINTR) {} }
typedef struct { int id, ms; } Job;
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t ready;
    sem_t slots;
    Job queue[CAPACITY];
    int head, tail, count, closing, completed;
    int workers, total, monitor_done;
    int state[MAX_WORKERS], current[MAX_WORKERS], finished[MAX_WORKERS];
    pthread_t threads[MAX_WORKERS];
    FILE *log;
    double start;
} Pool;
typedef struct { Pool *pool; int id; } Worker;
/* All events and queue transitions are serialized by the pool mutex. */
static void event(Pool *p, const char *type, int worker, int job) {
    if (worker >= 0) {
        if (!strcmp(type, "start")) { p->state[worker] = 2; p->current[worker] = job; }
        else if (!strcmp(type, "done")) { ++p->finished[worker]; p->state[worker] = 1; }
        else p->state[worker] = !strcmp(type, "stopped") ? 3 : 1;
    }
    if (p->log) fprintf(p->log, "{\"ms\":%.3f,\"type\":\"%s\",\"worker\":%d,\"job\":%d,\"queue\":%d,\"completed\":%d}\n",
                        (now() - p->start) * 1000, type, worker, job, p->count, p->completed);
}
/* Copy shared data under the mutex; print after unlocking so terminal I/O
   does not hold up workers. Only this monitor writes the live display. */
static void *monitor(void *arg) {
    Pool *p = arg;
    const char *term = getenv("TERM");
    int live = isatty(STDOUT_FILENO) && term && strcmp(term, "dumb");
    if (live) printf("\033[2J");
    for (;;) {
        int states[MAX_WORKERS], jobs[MAX_WORKERS], finished[MAX_WORKERS], queue[CAPACITY];
        check(pthread_mutex_lock(&p->mutex));
        int done = p->monitor_done, count = p->count, completed = p->completed;
        double elapsed = now() - p->start;
        for (int i = 0; i < p->workers; ++i) {
            states[i] = p->state[i]; jobs[i] = p->current[i]; finished[i] = p->finished[i];
        }
        for (int i = 0; i < count; ++i) queue[i] = p->queue[(p->head + i) % CAPACITY].id;
        check(pthread_mutex_unlock(&p->mutex));
        if (live || done) {
            /* Erase the previous frame before drawing: shorter lines must
               not leave old counter digits or queued job IDs behind. */
            if (live) printf("\033[H\033[J");
            printf("CONCURRENCY LAB | PID %ld | %.1f seconds\n", (long)getpid(), elapsed);
            printf("Workers: %d   Queued: %d/%d   Completed: %d/%d\n\n", p->workers, count, CAPACITY, completed, p->total);
            puts("WORKER   STATE       JOB       FINISHED");
            const char *names[] = {"STARTING", "IDLE", "BUSY", "STOPPED"};
            for (int i = 0; i < p->workers; ++i) {
                printf("%-8d %-11s ", i, names[states[i]]);
                if (states[i] == 2) printf("%-9d", jobs[i]); else printf("%-9s", "-");
                printf(" %d\n", finished[i]);
            }
            printf("\nWaiting jobs:");
            for (int i = 0; i < count; ++i) printf(" %d", queue[i]);
            printf("%s\n\n%s\n", count ? "" : " (empty)", done ? "All jobs finished. Workers joined safely." : "Live view: updates every 100 ms. BUSY means assigned a job.");
            if (live) printf("\033[J");
            fflush(stdout);
        }
        if (done) break;
        pause_ms(100);
    }
    return NULL;
}
static void *work(void *arg) {
    Worker *w = arg; Pool *p = w->pool;
    check(pthread_mutex_lock(&p->mutex));
    for (;;) {
        event(p, "idle", w->id, -1);
        while (!p->count && !p->closing) check(pthread_cond_wait(&p->ready, &p->mutex));
        if (!p->count && p->closing) break;
        Job j = p->queue[p->head]; p->head = (p->head + 1) % CAPACITY; --p->count;
        event(p, "start", w->id, j.id);
        if (sem_post(&p->slots)) { perror("sem_post"); exit(1); }
        check(pthread_mutex_unlock(&p->mutex));
        pause_ms(j.ms);
        check(pthread_mutex_lock(&p->mutex));
        ++p->completed; event(p, "done", w->id, j.id);
    }
    event(p, "stopped", w->id, -1);
    check(pthread_mutex_unlock(&p->mutex)); return NULL;
}
static double run_pool(int workers, int jobs, int ms, const char *path, int display) {
    Pool p = {0}; Worker args[MAX_WORKERS]; pthread_t viewer;
    p.workers = workers; p.total = jobs;
    check(pthread_mutex_init(&p.mutex, NULL)); check(pthread_cond_init(&p.ready, NULL));
    if (sem_init(&p.slots, 0, CAPACITY)) { perror("sem_init"); exit(1); }
    if (path && !(p.log = fopen(path, "w"))) { perror(path); exit(1); }
    p.start = now();
    if (display) check(pthread_create(&viewer, NULL, monitor, &p));
    for (int i = 0; i < workers; ++i) { args[i] = (Worker){&p, i}; check(pthread_create(&p.threads[i], NULL, work, &args[i])); }
    /* One producer; shutdown starts only after the last submission. */
    for (int i = 0; i < jobs; ++i) {
        while (sem_wait(&p.slots)) { if (errno != EINTR) { perror("sem_wait"); exit(1); } }
        check(pthread_mutex_lock(&p.mutex));
        p.queue[p.tail] = (Job){i, ms}; p.tail = (p.tail + 1) % CAPACITY; ++p.count;
        event(&p, "submit", -1, i); check(pthread_cond_signal(&p.ready));
        check(pthread_mutex_unlock(&p.mutex));
    }
    check(pthread_mutex_lock(&p.mutex)); p.closing = 1;
    check(pthread_cond_broadcast(&p.ready)); check(pthread_mutex_unlock(&p.mutex));
    for (int i = 0; i < workers; ++i) check(pthread_join(p.threads[i], NULL));
    double elapsed = now() - p.start;
    if (display) {
        check(pthread_mutex_lock(&p.mutex)); p.monitor_done = 1;
        check(pthread_mutex_unlock(&p.mutex)); check(pthread_join(viewer, NULL));
    }
    if (p.completed != jobs || p.count) { fprintf(stderr, "Pool invariant failed\n"); exit(1); }
    if (p.log && fclose(p.log)) { perror("close log"); exit(1); }
    check(pthread_cond_destroy(&p.ready)); check(pthread_mutex_destroy(&p.mutex));
    if (sem_destroy(&p.slots)) { perror("sem_destroy"); exit(1); }
    return elapsed;
}

static atomic_int counter;
static int safe_counter;
static void *count(void *arg) {
    int safe = *(int *)arg;
    for (int i = 0; i < 100000; ++i) {
        if (safe) atomic_fetch_add(&counter, 1);
        else { int value = atomic_load(&counter); atomic_store(&counter, value + 1); }
    }
    return NULL;
}
static void race(void) {
    for (safe_counter = 0; safe_counter <= 1; ++safe_counter) {
        pthread_t t[4]; atomic_store(&counter, 0);
        for (int i = 0; i < 4; ++i) check(pthread_create(&t[i], NULL, count, &safe_counter));
        for (int i = 0; i < 4; ++i) check(pthread_join(t[i], NULL));
        printf("%s: %d / 400000 (lost updates may vary)\n", safe_counter ? "Atomic increment" : "Separate load/store", atomic_load(&counter));
    }
}
static pthread_mutex_t a = PTHREAD_MUTEX_INITIALIZER, b = PTHREAD_MUTEX_INITIALIZER;
static pthread_barrier_t barrier;
static void *inverted(void *arg) {
    int id = *(int *)arg;
    pthread_mutex_t *first = id ? &b : &a, *second = id ? &a : &b;
    check(pthread_mutex_lock(first));
    int e = pthread_barrier_wait(&barrier); if (e != PTHREAD_BARRIER_SERIAL_THREAD) check(e);
    e = pthread_mutex_trylock(second);
    if (e == EBUSY) printf("Worker %d: second lock unavailable; blocking here could deadlock\n", id);
    else { check(e); check(pthread_mutex_unlock(second)); }
    /* Keep both first locks held until both attempts have completed. */
    e = pthread_barrier_wait(&barrier); if (e != PTHREAD_BARRIER_SERIAL_THREAD) check(e);
    check(pthread_mutex_unlock(first)); return NULL;
}
static void *ordered(void *unused) {
    (void)unused; check(pthread_mutex_lock(&a)); check(pthread_mutex_lock(&b));
    check(pthread_mutex_unlock(&b)); check(pthread_mutex_unlock(&a)); return NULL;
}
static void deadlock(void) {
    pthread_t t[2]; int ids[] = {0, 1}; check(pthread_barrier_init(&barrier, NULL, 2));
    for (int i = 0; i < 2; ++i) check(pthread_create(&t[i], NULL, inverted, &ids[i]));
    for (int i = 0; i < 2; ++i) check(pthread_join(t[i], NULL));
    check(pthread_barrier_destroy(&barrier));
    for (int i = 0; i < 2; ++i) check(pthread_create(&t[i], NULL, ordered, NULL));
    for (int i = 0; i < 2; ++i) check(pthread_join(t[i], NULL));
    puts("Fixed: both workers finished using the same lock order (A then B).");
}
static atomic_int ready_flag;
static pthread_mutex_t wait_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t wait_cond = PTHREAD_COND_INITIALIZER;
static void *signal_ready(void *unused) {
    (void)unused; pause_ms(200); check(pthread_mutex_lock(&wait_mutex));
    atomic_store(&ready_flag, 1); check(pthread_cond_signal(&wait_cond));
    check(pthread_mutex_unlock(&wait_mutex)); return NULL;
}
static void waiting(void) {
    for (int blocking = 0; blocking < 2; ++blocking) {
        pthread_t t; atomic_store(&ready_flag, 0); clock_t cpu = clock(); double start = now();
        check(pthread_create(&t, NULL, signal_ready, NULL));
        if (blocking) {
            check(pthread_mutex_lock(&wait_mutex));
            while (!atomic_load(&ready_flag)) check(pthread_cond_wait(&wait_cond, &wait_mutex));
            check(pthread_mutex_unlock(&wait_mutex));
        } else { while (!atomic_load(&ready_flag)) {} }
        check(pthread_join(t, NULL));
        printf("%s: wall %.1f ms, process CPU %.1f ms\n", blocking ? "Blocking" : "Spinning", (now()-start)*1000, (double)(clock()-cpu)*1000/CLOCKS_PER_SEC);
    }
}
static void *one_task(void *arg) { pause_ms(*(int *)arg); return NULL; }
static void bench(void) {
    const int jobs = 128; int ms = 5; pthread_t threads[128];
    puts("mode,workers,jobs,seconds");
    for (int n = 1; n <= 8; n *= 2) for (int trial = 0; trial < 3; ++trial)
        printf("pool,%d,%d,%.6f\n", n, jobs, run_pool(n, jobs, ms, NULL, 0));
    for (int trial = 0; trial < 3; ++trial) {
        double start = now();
        for (int i = 0; i < jobs; ++i) check(pthread_create(&threads[i], NULL, one_task, &ms));
        for (int i = 0; i < jobs; ++i) check(pthread_join(threads[i], NULL));
        printf("thread-per-task,128,%d,%.6f\n", jobs, now()-start);
    }
}
static int number(const char *s, int low, int high) {
    char *end; errno = 0; long n = strtol(s, &end, 10);
    if (errno || !*s || *end || n < low || n > high) { fprintf(stderr, "Expected integer %d..%d\n", low, high); exit(1); }
    return (int)n;
}
int main(int argc, char **argv) {
    if (argc >= 2 && !strcmp(argv[1], "pool") && argc <= 6) {
        int n = argc > 2 ? number(argv[2],1,8) : 4;
        int jobs = argc > 3 ? number(argv[3],0,10000) : 32;
        int ms = argc > 4 ? number(argv[4],0,10000) : 100;
        double elapsed = run_pool(n,jobs,ms,argc > 5 ? argv[5] : "events.jsonl", 1);
        printf("Completed %d jobs with %d workers in %.3f s\n", jobs, n, elapsed);
    } else if (argc == 2 && !strcmp(argv[1], "race")) race();
    else if (argc == 2 && !strcmp(argv[1], "deadlock")) deadlock();
    else if (argc == 2 && !strcmp(argv[1], "wait")) waiting();
    else if (argc == 2 && !strcmp(argv[1], "bench")) bench();
    else { fprintf(stderr, "Usage: %s pool [workers 1..8] [jobs 0..10000] [ms 0..10000] [log path]\n       %s race|deadlock|wait|bench\n", argv[0], argv[0]); return 1; }
    return 0;
}
