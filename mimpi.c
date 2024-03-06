/**
 * This file is for implementation of MIMPI library.
 * */

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <errno.h>
#include <stdint.h>
#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"


struct metadata {
    int count;
    int tag;
};
typedef struct metadata metadata;

#define GR_READY 1
#define GR_FINALIZE 2

struct recv_queue {
    metadata meta;
    void* data;
    struct recv_queue* next;
};
typedef struct recv_queue recv_queue;

struct sent_queue {
    metadata meta;
    struct sent_queue* next;
};
typedef struct sent_queue sent_q;

struct queue {
    recv_queue** begin_data_queue;
    recv_queue** end_data_queue;
    sem_t mutex;
    sem_t wait;
    bool* receiver_running;
    bool waiting;
    int needed_tag;
    int needed_source;
    int needed_count;
    void* wait_data;
    int got_data;
    metadata other_waiting[16];
    sent_q* sent_queue[16];
};
typedef struct queue queue;

static queue rec_data;
static int rank, world_size;
static pthread_t* rec_threads;
static bool gr_comm;
static bool deadlock;

// first file descriptor is ZEROFD, there are 4*(world_size-1) channels
// each (world_size-1) descriptors are in 1 group, in order:
// p-p in, p-p out, group in, group out.
static int ppfdin(int source) {
    if (source > rank) {
        source--;
    }
    return ZEROFD + source;
}

static int ppfdout(int dest) {
    if (dest > rank) {
        dest--;
    }
    return ZEROFD + world_size - 1 + dest;
}

static int grdatafdout(int dest) {
    if (dest > rank) {
        dest--;
    }
    return GR_DATA_OUT + dest;
}

static MIMPI_Retcode trysend(int fd, const void* buf, size_t bcount) {
    while (bcount > 0) {
        int bytesent = chsend(fd, buf, bcount);
        if (bytesent == -1 && errno == EPIPE) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        ASSERT_SYS_OK(bytesent);
        bcount -= bytesent;
        buf += bytesent;
    }
    return MIMPI_SUCCESS;
}

static int tryrecv(int fd, void* buf, size_t bcount) {
    while (bcount > 0) {
        int byterecv = chrecv(fd, buf, bcount);
        ASSERT_SYS_OK(byterecv);
        if (byterecv == 0) {
            return 0;
        }
        bcount -= byterecv;
        buf += byterecv;
    }
    return 1;
}

static void add_sent_queue (int dest, metadata md) {
    sent_q* temp = (sent_q*) malloc(sizeof(sent_q));
    temp->meta.count = md.count;
    temp->meta.tag = md.tag;
    temp->next = rec_data.sent_queue[dest];
    rec_data.sent_queue[dest] = temp;
}

static bool remove_sent (int dest, metadata md) {
    if (rec_data.sent_queue[dest] == NULL) {
        return false;
    }
    sent_q* last = NULL;
    sent_q* curr = rec_data.sent_queue[dest];
    while (curr != NULL) {
        if (curr->meta.count == md.count && curr->meta.tag == md.tag) {
            if (last == NULL) {
                rec_data.sent_queue[dest] = NULL;
            }
            else {
                last->next = curr->next;
            }
            free(curr);
            ASSERT_SYS_OK(sem_post(&rec_data.mutex));

            return true;
        }
        last = curr;
        curr = curr->next;
    }
    return false;
}


static void write_to_queue(int source, metadata meta, void* data) {
    sem_wait(&rec_data.mutex);
    if (rec_data.waiting && meta.count == rec_data.needed_count &&
        (meta.tag == rec_data.needed_tag || rec_data.needed_tag == 0) && source == rec_data.needed_source) {
        memcpy(rec_data.wait_data, data, meta.count);
        rec_data.got_data = 1;
        rec_data.waiting = false;
        free(data);
        sem_post(&rec_data.mutex);
        sem_post(&rec_data.wait);
        return;
    }

    recv_queue* new = (recv_queue*)malloc(sizeof(recv_queue));
    assert(new != NULL);
    new->meta.count = meta.count;
    new->meta.tag = meta.tag;
    new->data = data;
    new->next = NULL;

    if (rec_data.begin_data_queue[source] == NULL) {
        rec_data.begin_data_queue[source] = new;
    }
    else {
        rec_data.end_data_queue[source]->next = new;
    }

    rec_data.end_data_queue[source] = new;

    sem_post(&rec_data.mutex);
}


static void* receiver(void* source) {
    int id = *(int*)source;
    free(source);
    int* retcode = (int*)malloc(sizeof(int));
    assert(retcode != NULL);
    while (true) {
        metadata md;
        if (tryrecv(ppfdin(id), &md, sizeof(metadata)) == 0) {
            *retcode = 1;
            sem_wait(&rec_data.mutex);
            rec_data.receiver_running[id] = false;
            sem_post(&rec_data.mutex);
            if (rec_data.waiting && rec_data.needed_source == id) {
                sem_post(&rec_data.wait);
            }
            ASSERT_SYS_OK(close(ppfdin(id)));

            return retcode;
        }
        if (md.tag == -1) {
            tryrecv(ppfdin(id), &md, sizeof(metadata));
            sem_wait(&rec_data.mutex);
            if (!remove_sent(id, md)) {
                if (rec_data.waiting && rec_data.needed_source == id) {
                    rec_data.got_data = -1;
                    rec_data.waiting = false;
                    sem_post(&rec_data.wait);

                }

                else {
                    rec_data.other_waiting[id].count = md.count;
                    rec_data.other_waiting[id].tag = md.tag;
                }
            }
            sem_post(&rec_data.mutex);
            continue;
        }
        if (md.tag == -2) {
            sem_wait(&rec_data.mutex);
            rec_data.got_data = -1;
            rec_data.waiting = false;
            sem_post(&rec_data.wait);
            sem_post(&rec_data.mutex);
            continue;
        }

        void* data = malloc(md.count);
        assert(data != NULL);
        if (tryrecv(ppfdin (id), data, md.count) == 0) {
            *retcode = 1;
            sem_wait(&rec_data.mutex);
            rec_data.receiver_running[id] = false;
            sem_post(&rec_data.mutex);
            if (rec_data.waiting && rec_data.needed_source == id) {
                sem_post(&rec_data.wait);
            }
            ASSERT_SYS_OK(close(ppfdin(id)));
            return retcode;
        }

        write_to_queue(id, md, data);
    }
}

static int take_data(void *data, int count, int source, int tag) {
    sem_wait(&rec_data.mutex);
    recv_queue* last = NULL;
    for (recv_queue* i = rec_data.begin_data_queue[source]; i != NULL;i = i->next) {
        if (i->meta.count == count && (i->meta.tag == tag || tag == 0)) {
            memcpy(data, i->data, count);

            if (i == rec_data.begin_data_queue[source]) {
                rec_data.begin_data_queue[source] = i->next;
            }
            if (i == rec_data.end_data_queue[source]) {
                rec_data.end_data_queue[source] = last;
            }

            if (last != NULL) {
                last->next = i->next;
            }

            free(i->data);
            free(i);
            sem_post(&rec_data.mutex);
            return 1;
        }
        last = i;
    }

    return 0;
}

static int take_from_queue(void *data, int count, int source, int tag) {
    if (take_data(data, count, source, tag)) {
        return 1;
    }

    if (deadlock) {
        if (rec_data.other_waiting[source].tag > -1) {
            rec_data.other_waiting[source].tag = -1;
            rec_data.other_waiting[source].count = -1;
            bool other_running = rec_data.receiver_running[source];
            sem_post(&rec_data.mutex);
            metadata md;
            md.tag = -2;
            md.count = 1;
            if (other_running)
                trysend(ppfdout(source), &md, sizeof(metadata));
            return MIMPI_ERROR_DEADLOCK_DETECTED;
        }
    }


    if (!rec_data.receiver_running[source]) {
        sem_post(&rec_data.mutex);
        return 0;
    }

    rec_data.needed_source = source;
    rec_data.needed_tag = tag;
    rec_data.needed_count = count;
    rec_data.wait_data = data;
    rec_data.waiting = true;
    rec_data.got_data = 0;
    sem_post(&rec_data.mutex);

    if (deadlock) {
        metadata md;
        md.tag = -1;
        md.count = sizeof(metadata);
        trysend(ppfdout(source), &md, sizeof(metadata));
        md.tag = tag;
        md.count = count;
        trysend(ppfdout(source), &md, sizeof(metadata));
    }


    sem_wait(&rec_data.wait);

    sem_wait(&rec_data.mutex);
    rec_data.waiting = false;

    if (rec_data.got_data == 1) {
        sem_post(&rec_data.mutex);
        return 1;
    }
    else if (rec_data.got_data == 0) {
        sem_post(&rec_data.mutex);
        return 0;
    }
    else {
        sem_post(&rec_data.mutex);
        return MIMPI_ERROR_DEADLOCK_DETECTED;
    }
}


void MIMPI_Init(bool enable_deadlock_detection) {
    deadlock = enable_deadlock_detection;
    channels_init();
    rank = atoi(getenv("MIMPI_RANK"));
    world_size = atoi(getenv("MIMPI_WORLD_SIZE"));
    unsetenv("MIMPI_WORLD_SIZE");
    unsetenv("MIMPI_RANK");

    rec_data.begin_data_queue = (recv_queue**) malloc(world_size * sizeof(recv_queue*));
    rec_data.end_data_queue = (recv_queue**) malloc(world_size * sizeof(recv_queue*));
    rec_data.receiver_running = (bool*) malloc(world_size * sizeof(bool));
    assert(rec_data.begin_data_queue != NULL && rec_data.end_data_queue != NULL && rec_data.receiver_running != NULL);
    for (int i = 0; i < world_size; i++) {
        rec_data.begin_data_queue[i] = NULL;
        rec_data.end_data_queue[i] = NULL;
        rec_data.receiver_running[i] = true;
        rec_data.sent_queue[i] = NULL;
    }
    ASSERT_SYS_OK(sem_init(&rec_data.mutex, 0, 1));
    ASSERT_SYS_OK(sem_init(&rec_data.wait, 0, 0));
    rec_data.waiting = false;
    rec_data.needed_tag = -1;
    rec_data.needed_count = -1;
    rec_data.needed_source = -1;

    gr_comm = true;

    rec_threads = (pthread_t*) malloc(world_size * sizeof(pthread_t));
    assert(rec_threads != NULL);
    pthread_attr_t attr;
    ASSERT_ZERO(pthread_attr_init(&attr));
    ASSERT_ZERO(pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE));

    for (int i = 0; i < world_size; i++) {
        rec_data.other_waiting[i].tag = -1;
        rec_data.other_waiting[i].count = -1;
    }

    for (int i = 0; i < world_size; i++) {
        if (i != rank) {
            int* rec_rank = (int*) malloc(sizeof(int));
            assert(rec_rank != NULL);
            *rec_rank = i;
            ASSERT_ZERO(pthread_create(&rec_threads[i], &attr, receiver, rec_rank));
        }
    }
}

void MIMPI_Finalize() {
    // close sending channels
    for (int i = 0; i < world_size; i++) {
        if (i != rank) {
            ASSERT_SYS_OK(close(ppfdout(i)));
        }
    }
    // WYSLAC INNYM PROCESOM W GRUPOWEJ ZE SKONCZYLEM DZIALAC


    if (gr_comm) {
        int treepos = rank+1;
        int root = treepos/2;
        int leftc = treepos*2;
        int rightc = treepos*2+1;
        char comm = GR_FINALIZE;
        if (root > 0) {
            trysend(GR_ROOT_OUT, &comm, sizeof(char));
            ASSERT_SYS_OK(close(GR_ROOT_IN));
            ASSERT_SYS_OK(close(GR_ROOT_OUT));
        }
        if (leftc <= world_size) {
            trysend(GR_LEFT_OUT, &comm, sizeof(char));
            ASSERT_SYS_OK(close(GR_LEFT_IN));
            ASSERT_SYS_OK(close(GR_LEFT_OUT));
        }
        if (rightc <= world_size) {
            trysend(GR_RIGHT_OUT, &comm, sizeof(char));
            ASSERT_SYS_OK(close(GR_RIGHT_IN));
            ASSERT_SYS_OK(close(GR_RIGHT_OUT));
        }
    }

    ASSERT_SYS_OK(close(GR_DATA_IN));
    for (int i = 0; i < world_size - 1; i++) {
        ASSERT_SYS_OK(close(GR_DATA_OUT + i));
    }


    // wait for all threads, then free memory, semaphores
    for (int i = 0; i < world_size; i++) {
        if (i != rank) {
            int* status;
            ASSERT_ZERO(pthread_join(rec_threads[i], (void**)&status));
            free(status);
        }
    }

//     free all memory
    for (int i = 0; i < world_size; i++) {
        recv_queue* list = rec_data.begin_data_queue[i];
        while (list != NULL) {
            free(list->data);
            recv_queue* temp = list;
            list = list->next;
            free(temp);
        }
    }

    for (int i = 0; i < world_size; i++) {
        sent_q * list = rec_data.sent_queue[i];
        while (list != NULL) {
            sent_q* temp = list;
            list = list->next;
            free(temp);
        }
    }
    free(rec_data.begin_data_queue);
    free(rec_data.end_data_queue);
    free(rec_threads);
    free(rec_data.receiver_running);
    ASSERT_SYS_OK(sem_destroy(&rec_data.mutex));
    ASSERT_SYS_OK(sem_destroy(&rec_data.wait));


//    print_open_descriptors();

    channels_finalize();
}

int MIMPI_World_size() {
    return world_size;
}

int MIMPI_World_rank() {
    return rank;
}

MIMPI_Retcode MIMPI_Send(
        void const *data,
        int count,
        int destination,
        int tag
) {
    if (destination == rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }

    if (destination < 0 || destination >= world_size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    metadata md;
    md.count = count;
    md.tag = tag;
    
    if (deadlock) {
        ASSERT_SYS_OK(sem_wait(&rec_data.mutex));
        if (!rec_data.receiver_running[destination]) {
            ASSERT_SYS_OK(sem_post(&rec_data.mutex));
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        ASSERT_SYS_OK(sem_post(&rec_data.mutex));

        ASSERT_SYS_OK(sem_wait(&rec_data.mutex));
        if (rec_data.other_waiting[destination].tag == tag && rec_data.other_waiting[destination].count == count) {
            rec_data.other_waiting[destination].tag = -1;
            rec_data.other_waiting[destination].count = -1;
        }

        else {
            add_sent_queue(destination, md);
        }
        ASSERT_SYS_OK(sem_post(&rec_data.mutex));
    }


    if (trysend(ppfdout(destination), &md, sizeof(metadata)) == MIMPI_ERROR_REMOTE_FINISHED) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    if (trysend(ppfdout(destination), data, count) == MIMPI_ERROR_REMOTE_FINISHED) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    return MIMPI_SUCCESS;
}


MIMPI_Retcode MIMPI_Recv(
        void *data,
        int count,
        int source,
        int tag
) {
    if (source == rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }

    if (source < 0 || source >= world_size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    int res = take_from_queue(data, count, source, tag);
    if (res == 0) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    else if (res == MIMPI_ERROR_DEADLOCK_DETECTED) {
        return MIMPI_ERROR_DEADLOCK_DETECTED;
    }

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    if (!gr_comm) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    int treepos = rank + 1;
    int root = treepos / 2;
    int leftc = treepos * 2;
    int rightc = treepos * 2 + 1;

    char comm1 = GR_READY, comm2 = GR_READY;
    if (leftc <= world_size) {
        tryrecv(GR_LEFT_IN, &comm1, sizeof(char));
    }

    if (rightc <= world_size) {
        tryrecv(GR_RIGHT_IN, &comm2, sizeof(char));
    }

    if (comm1 == GR_FINALIZE || comm2 == GR_FINALIZE) {
        char mycomm = GR_FINALIZE;
        gr_comm = false;
        if (root > 0) {
            trysend(GR_ROOT_OUT, &mycomm, sizeof(char));
            ASSERT_SYS_OK(close(GR_ROOT_IN));
            ASSERT_SYS_OK(close(GR_ROOT_OUT));
        }
        if (leftc <= world_size) {
            trysend(GR_LEFT_OUT, &mycomm, sizeof(char));
            ASSERT_SYS_OK(close(GR_LEFT_IN));
            ASSERT_SYS_OK(close(GR_LEFT_OUT));
        }
        if (rightc <= world_size) {
            trysend(GR_RIGHT_OUT, &mycomm, sizeof(char));
            ASSERT_SYS_OK(close(GR_RIGHT_IN));
            ASSERT_SYS_OK(close(GR_RIGHT_OUT));
        }
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    char mycomm = GR_READY;

    if (root > 0) {
        trysend(GR_ROOT_OUT, &mycomm, sizeof(char));
        tryrecv(GR_ROOT_IN, &mycomm, sizeof(char));
    }

    if (leftc <= world_size) {
        trysend(GR_LEFT_OUT, &mycomm, sizeof(char));
    }
    if (rightc <= world_size) {
        trysend(GR_RIGHT_OUT, &mycomm, sizeof(char));
    }

    if (mycomm == GR_FINALIZE) {
        gr_comm = false;
        if (root > 0) {
            ASSERT_SYS_OK(close(GR_ROOT_IN));
            ASSERT_SYS_OK(close(GR_ROOT_OUT));
        }
        if (leftc <= world_size) {
            ASSERT_SYS_OK(close(GR_LEFT_IN));
            ASSERT_SYS_OK(close(GR_LEFT_OUT));
        }
        if (rightc <= world_size) {
            ASSERT_SYS_OK(close(GR_RIGHT_IN));
            ASSERT_SYS_OK(close(GR_RIGHT_OUT));
        }
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(
        void *data,
        int count,
        int root_bcast
) {
    if (!gr_comm) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    int treepos = rank + 1;
    int root = treepos / 2;
    int leftc = treepos * 2;
    int rightc = treepos * 2 + 1;

    char comm1 = GR_READY, comm2 = GR_READY;
    if (leftc <= world_size) {
        tryrecv(GR_LEFT_IN, &comm1, sizeof(char));
    }

    if (rightc <= world_size) {
        tryrecv(GR_RIGHT_IN, &comm2, sizeof(char));
    }

    if (comm1 == GR_FINALIZE || comm2 == GR_FINALIZE) {
        char comm = GR_FINALIZE;
        gr_comm = false;
        if (root > 0) {
            trysend(GR_ROOT_OUT, &comm, sizeof(char));
            ASSERT_SYS_OK(close(GR_ROOT_IN));
            ASSERT_SYS_OK(close(GR_ROOT_OUT));
        }
        if (leftc <= world_size) {
            trysend(GR_LEFT_OUT, &comm, sizeof(char));
            ASSERT_SYS_OK(close(GR_LEFT_IN));
            ASSERT_SYS_OK(close(GR_LEFT_OUT));
        }
        if (rightc <= world_size) {
            trysend(GR_RIGHT_OUT, &comm, sizeof(char));
            ASSERT_SYS_OK(close(GR_RIGHT_IN));
            ASSERT_SYS_OK(close(GR_RIGHT_OUT));
        }
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    char mycomm = GR_READY;
    void* recv = malloc(sizeof(char) + count);
    if (root == 0) {
        if (root_bcast == rank) {
            memcpy(recv, &mycomm, sizeof(char));
            memcpy(recv+sizeof(char), data, count);
        }
        else {
            tryrecv(GR_DATA_IN, recv + sizeof(char), count);
            ((char*)recv)[0] = mycomm;
        }
    }

    else if (root > 0) {
        trysend(GR_ROOT_OUT, &mycomm, sizeof(char));
        if (root_bcast == rank) {
            trysend(grdatafdout(0), data, count);
        }

        tryrecv(GR_ROOT_IN, recv, sizeof(char)+count);
        memcpy(&mycomm, recv, sizeof(char));
    }

    if (leftc <= world_size) {
        trysend(GR_LEFT_OUT, recv, sizeof(char) + count);
    }
    if (rightc <= world_size) {
        trysend(GR_RIGHT_OUT, recv, sizeof(char) + count);
    }

    if (mycomm == GR_FINALIZE) {
        gr_comm = false;
        if (root > 0) {
            ASSERT_SYS_OK(close(GR_ROOT_IN));
            ASSERT_SYS_OK(close(GR_ROOT_OUT));
        }
        if (leftc <= world_size) {
            ASSERT_SYS_OK(close(GR_LEFT_IN));
            ASSERT_SYS_OK(close(GR_LEFT_OUT));
        }
        if (rightc <= world_size) {
            ASSERT_SYS_OK(close(GR_RIGHT_IN));
            ASSERT_SYS_OK(close(GR_RIGHT_OUT));
        }
        free(recv);
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    else {
        memcpy(data, recv+sizeof(char), count);
        free(recv);
    }
    return MIMPI_SUCCESS;
}

static void exec_MIMPI_Op(uint8_t* res, const uint8_t* data1, const  uint8_t* data2, const uint8_t* data3, int count, MIMPI_Op op) {
    for (int i = 0; i < count; i++) {
        uint8_t cell = data1[i];
        if (data2) {
            if (op == MIMPI_MAX) {
                cell = cell > data2[i] ? cell : data2[i];
            }
            else if (op == MIMPI_MIN) {
                cell = cell < data2[i] ? cell : data2[i];
            }
            else if (op == MIMPI_SUM) {
                cell += data2[i];
            }
            else {
                cell *= data2[i];
            }
        }
        if (data3) {
            if (op == MIMPI_MAX) {
                cell = cell > data3[i] ? cell : data3[i];
            }
            else if (op == MIMPI_MIN) {
                cell = cell < data3[i] ? cell : data3[i];
            }
            else if (op == MIMPI_SUM) {
                cell += data3[i];
            }
            else {
                cell *= data3[i];
            }
        }

        res[i] = cell;
    }
}

MIMPI_Retcode MIMPI_Reduce(
        void const *send_data,
        void *recv_data,
        int count,
        MIMPI_Op op,
        int root_reduce
) {
    if (!gr_comm) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    int treepos = rank + 1;
    int root = treepos / 2;
    int leftc = treepos * 2;
    int rightc = treepos * 2 + 1;

    void* comm1 = NULL;
    void* comm2 = NULL;
    if (leftc <= world_size) {
        comm1 = malloc(count + 1);
        tryrecv(GR_LEFT_IN, comm1, count + sizeof(char));
    }

    if (rightc <= world_size) {
        comm2 = malloc(count + 1);
        tryrecv(GR_RIGHT_IN, comm2, count + sizeof(char));
    }

    char stat1 = comm1 == NULL ? GR_READY : *(char*)comm1;
    char stat2 = comm2 == NULL ? GR_READY : *(char*)comm2;
    if (stat1 == GR_FINALIZE || stat2 == GR_FINALIZE) {
        char comm = GR_FINALIZE;
        gr_comm = false;
        if (root > 0) {
            trysend(GR_ROOT_OUT, &comm, sizeof(char));
            ASSERT_SYS_OK(close(GR_ROOT_IN));
            ASSERT_SYS_OK(close(GR_ROOT_OUT));
        }
        if (leftc <= world_size) {
            trysend(GR_LEFT_OUT, &comm, sizeof(char));
            ASSERT_SYS_OK(close(GR_LEFT_IN));
            ASSERT_SYS_OK(close(GR_LEFT_OUT));
        }
        if (rightc <= world_size) {
            trysend(GR_RIGHT_OUT, &comm, sizeof(char));
            ASSERT_SYS_OK(close(GR_RIGHT_IN));
            ASSERT_SYS_OK(close(GR_RIGHT_OUT));
        }
        free(comm1);
        free(comm2);
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    char mycomm = GR_READY;
    void* data1 = comm1 == NULL ? NULL : comm1+1;
    void* data2 = comm2 == NULL ? NULL : comm2+1;
    void* res = malloc(count + 1);
    *(char*)res = GR_READY;
    exec_MIMPI_Op(res+1, send_data, data1, data2, count, op);
    free(comm1);
    free(comm2);
    if (root > 0) {
        trysend(GR_ROOT_OUT, res, count + 1);
        // free(res);

        tryrecv(GR_ROOT_IN, &mycomm, sizeof(char));
    }

    if (leftc <= world_size) {
        trysend(GR_LEFT_OUT, &mycomm, sizeof(char));
    }
    if (rightc <= world_size) {
        trysend(GR_RIGHT_OUT, &mycomm, sizeof(char));
    }

    if (mycomm == GR_FINALIZE) {
        gr_comm = false;
        if (root > 0) {
            ASSERT_SYS_OK(close(GR_ROOT_IN));
            ASSERT_SYS_OK(close(GR_ROOT_OUT));
        }
        if (leftc <= world_size) {
            ASSERT_SYS_OK(close(GR_LEFT_IN));
            ASSERT_SYS_OK(close(GR_LEFT_OUT));
        }
        if (rightc <= world_size) {
            ASSERT_SYS_OK(close(GR_RIGHT_IN));
            ASSERT_SYS_OK(close(GR_RIGHT_OUT));
        }
        free(res);
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    if (root == 0 && root_reduce == rank) {
        memcpy(recv_data, res + 1, count);
    }

    else if (root == 0) {
        trysend(grdatafdout(root_reduce), res + 1, count);
    }


    else if (root_reduce == rank) {
        tryrecv(GR_DATA_IN, recv_data, count);
    }
    free(res);

    return MIMPI_SUCCESS;
}