/* Run only against the disposable volume created by protocols.py.
 * Build: cc tests/libiscsi_probe.c $(pkg-config --cflags --libs libiscsi) -o libiscsi-probe
 * Uses libiscsi's protocol implementation, not the Python wire test client.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iscsi/iscsi.h>
#include <iscsi/scsi-lowlevel.h>

static void check(struct iscsi_context *ctx, struct scsi_task *task, const char *op)
{
    if (!task || task->status != SCSI_STATUS_GOOD) {
        fprintf(stderr, "%s failed: %s\n", op, iscsi_get_error(ctx));
        exit(1);
    }
}

int main(int argc, char **argv)
{
    if (argc != 2 || !getenv("YADB_TEST_SECRET")) return 2;
    struct iscsi_context *ctx = iscsi_create_context("iqn.2026-11.test:libiscsi");
    if (!ctx) return 1;
    iscsi_set_timeout(ctx, 15);
    iscsi_set_noautoreconnect(ctx, 1);
    iscsi_set_targetname(ctx, "iqn.2026-11.io.yadb:billing:vm-root");
    iscsi_set_session_type(ctx, ISCSI_SESSION_NORMAL);
    iscsi_set_header_digest(ctx, ISCSI_HEADER_DIGEST_NONE);
    iscsi_set_initiator_username_pwd(ctx, "backend", getenv("YADB_TEST_SECRET"));
    iscsi_set_immediate_data(ctx, ISCSI_IMMEDIATE_DATA_NO);
    iscsi_set_initial_r2t(ctx, ISCSI_INITIAL_R2T_YES);
    if (iscsi_full_connect_sync(ctx, argv[1], 0)) {
        fprintf(stderr, "login failed: %s\n", iscsi_get_error(ctx));
        return 1;
    }
    const unsigned length = 524288;
    unsigned char *data = malloc(length);
    if (!data) return 1;
    for (unsigned i = 0; i < length; i++) data[i] = (unsigned char)(i * 37 + 11);
    struct scsi_task *task = iscsi_write16_sync(ctx, 0, 32, data, length, 512, 0, 0, 1, 0, 0);
    check(ctx, task, "WRITE(16) R2T");
    scsi_free_scsi_task(task);
    task = iscsi_read16_sync(ctx, 0, 32, length, 512, 0, 0, 0, 0, 0);
    check(ctx, task, "READ(16)");
    if (task->datain.size != (int)length || memcmp(task->datain.data, data, length)) {
        fprintf(stderr, "read-back bytes differ\n");
        return 1;
    }
    scsi_free_scsi_task(task);
    free(data);
    if (iscsi_logout_sync(ctx)) return 1;
    iscsi_destroy_context(ctx);
    puts("PASS libiscsi CHAP, WRITE(16) R2T/Data-Out and READ(16) byte verification");
    return 0;
}
