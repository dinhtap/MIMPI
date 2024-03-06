/**
 * This file is for implementation of mimpirun program.
 * */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include "mimpi_common.h"
#include "channel.h"

int main(int argc, char** argv) {
    int n = atoi(argv[1]);
    ASSERT_SYS_OK(setenv("MIMPI_WORLD_SIZE", argv[1], 1));
    int ppchannels[16][16][2];
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            ppchannels[i][j][0] = -1;
            ppchannels[i][j][1] = -1;
        }
    }

    int grdatachannels[16][2];
    for (int i = 0; i < n; i++) {
        ASSERT_SYS_OK(channel(grdatachannels[i]));
    }

    int grchannels[15][2][2];
    for (int i = 0; i < n-1; i++) {
        ASSERT_SYS_OK(channel(grchannels[i][0]));
        ASSERT_SYS_OK(channel(grchannels[i][1]));
    }

    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            if (i != j) {
                if (ppchannels[i][j][0] == -1) {
                    ASSERT_SYS_OK(channel(ppchannels[i][j]));
                }
                if (ppchannels[j][i][0] == -1) {
                    ASSERT_SYS_OK(channel(ppchannels[j][i]));
                }
            }
        }

        pid_t id = fork();

        if (!id) {
            int fd1 = ZEROFD;
            int fd2 = fd1 + n - 1;
            for (int k = 0; k < n; k++) {
                if (i != k) {
                    ASSERT_SYS_OK(dup2(ppchannels[i][k][0], fd1++));
                    ASSERT_SYS_OK(dup2(ppchannels[k][i][1], fd2++));
                }
            }

            int treepos = i+1;
            int root = treepos/2;
            int leftc = treepos*2;
            int rightc = treepos*2+1;
            if (root > 0) {
                ASSERT_SYS_OK(dup2(grchannels[treepos-2][0][0], GR_ROOT_IN));
                ASSERT_SYS_OK(dup2(grchannels[treepos-2][1][1], GR_ROOT_OUT));
            }
            if (leftc <= n) {
                ASSERT_SYS_OK(dup2(grchannels[leftc-2][1][0], GR_LEFT_IN));
                ASSERT_SYS_OK(dup2(grchannels[leftc-2][0][1], GR_LEFT_OUT));
            }
            if (rightc <= n) {
                ASSERT_SYS_OK(dup2(grchannels[rightc-2][1][0], GR_RIGHT_IN));
                ASSERT_SYS_OK(dup2(grchannels[rightc-2][0][1], GR_RIGHT_OUT));
            }

            fd1 = GR_DATA_OUT;
            ASSERT_SYS_OK(dup2(grdatachannels[i][0], GR_DATA_IN));
            for (int j = 0; j < n; j++) {
                if (j != i) {
                    ASSERT_SYS_OK(dup2(grdatachannels[j][1], fd1++));
                }
            }

            for (int j = 0; j < n; j++) {
                for (int k = 0; k < n; k++) {
                    if (ppchannels[j][k][0] != -1) {
                        close(ppchannels[j][k][0]);
                        close(ppchannels[j][k][1]);
                    }
                }
            }

            for (int j = 0; j < n-1; j++) {
                ASSERT_SYS_OK(close(grchannels[j][0][0]));
                ASSERT_SYS_OK(close(grchannels[j][1][0]));
                ASSERT_SYS_OK(close(grchannels[j][0][1]));
                ASSERT_SYS_OK(close(grchannels[j][1][1]));
            }

            for (int j = 0; j < n; j++) {
                ASSERT_SYS_OK(close(grdatachannels[j][0]));
                ASSERT_SYS_OK(close(grdatachannels[j][1]));
            }

            char rank_string[3];
            int retr = snprintf(rank_string, sizeof rank_string, "%d", i);
            if (retr < 0 || retr >= (int)sizeof(rank_string))
                fatal("snprintf failed");

            setenv("MIMPI_RANK", rank_string, 1);
            ASSERT_SYS_OK(execvp(argv[2], argv + 2));
        }
        else {
            for (int k = 0; k < n; k++) {
                if (i != k) {
                    ASSERT_SYS_OK(close(ppchannels[i][k][0]));
                    ASSERT_SYS_OK(close(ppchannels[k][i][1]));

                }
            }
        }
    }

    for (int i = 0; i < n-1; i++) {
        ASSERT_SYS_OK(close(grchannels[i][0][0]));
        ASSERT_SYS_OK(close(grchannels[i][1][0]));
        ASSERT_SYS_OK(close(grchannels[i][0][1]));
        ASSERT_SYS_OK(close(grchannels[i][1][1]));
    }

    for (int i = 0; i < n; i++) {
        ASSERT_SYS_OK(close(grdatachannels[i][0]));
        ASSERT_SYS_OK(close(grdatachannels[i][1]));
    }

    ASSERT_SYS_OK(unsetenv("MIMPI_WORLD_SIZE"));
    ASSERT_SYS_OK(unsetenv("MIMPI_RANK"));

    for (int i = 0; i < n; i++) {
        int retcode = 1;
        wait(&retcode);
        if (retcode != 0) {
            return 1;
        }
    }
    return 0;
}