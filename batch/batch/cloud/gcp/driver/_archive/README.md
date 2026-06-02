Superseded by docker-run bootstrap (Phase 1). To be deleted after Phase 3 sign-off.

`lambda_worker_setup.sh` was the chroot/squashfs-era one-time VM setup script.
It has been replaced by the `transfer_credentials_and_configure_docker` +
`run_worker_container` methods in `batch/batch/driver/instance.py`.
