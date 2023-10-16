#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <config.h>
#include <limits.h>
#include <stdio.h>
#include <ext2fs/ext2fs.h>
#ifdef HAVE_ZFS
#include <libzfs/libzfs.h>
#include <sys/zfs_context.h>
#endif
#include <sys/param.h>
#include "clf_constant.h"

#ifdef HAVE_ZFS
libzfs_handle_t *g_zfs;
#endif

#define LDISKFS_OPENFS_FLAGS (EXT2_FLAG_64BITS | EXT2_FLAG_SKIP_MMP | \
			      EXT2_FLAG_SUPER_ONLY)

const char *occupied_string = "Occupied by host: ";
static int
ldiskfs_check_mountable(const char *dev, ext2_filsys fs)
{
	int retval;
	unsigned int seq;
	int wait_time;
	int mmp_check_interval;
	struct mmp_struct *mmp;

	if (fs->mmp_buf == NULL) {
		retval = ext2fs_get_mem(fs->blocksize, &fs->mmp_buf);
		if (retval) {
			fprintf(stderr, "failed to alloc MMP buffer\n");
			return CSM_AGAIN;
		}
	}
	mmp = fs->mmp_buf;

	retval = ext2fs_mmp_read(fs, fs->super->s_mmp_block, mmp);
	if (retval == EXT2_ET_OP_NOT_SUPPORTED) {
		fprintf(stderr,
			"MMP feature is not supported by device [%s]\n",
			dev);
		return CSM_UNSUPPORTED;
	}
	if (retval) {
		fprintf(stderr,
			"failed to read MMP block from device [%s]: %s\n",
			dev, error_message(retval));
		return CSM_AGAIN;
	}

	mmp_check_interval = mmp->mmp_check_interval;
	if (mmp_check_interval < EXT4_MMP_MIN_CHECK_INTERVAL)
		mmp_check_interval = EXT4_MMP_MIN_CHECK_INTERVAL;

	seq = mmp->mmp_seq;
	if (seq == EXT4_MMP_SEQ_CLEAN) {
		printf("Lustre service on device [%s] is mountable\n", dev);
		return CSM_MOUNTABLE;
	}

	/*
	 * fsck is running on the filesystem, it might be able to
	 * mountable later.
	 */
	if (seq == EXT4_MMP_SEQ_FSCK)
		return CSM_AGAIN;

	wait_time = MIN(mmp_check_interval * 2 + 1,
			mmp_check_interval + 60);

	/* Print MMP interval if more than 20 secs. */
	fprintf(stderr,
		"checking MMP, max wait time is [%d] seconds\n",
		wait_time);

	/*
	 * The MMP could be changed at any second, so do not sleep for too
	 * long.
	 */
	while (wait_time > 0) {
		sleep(1);
		wait_time--;

		retval = ext2fs_mmp_read(fs, fs->super->s_mmp_block, mmp);
		if (retval) {
			fprintf(stderr,
				"failed to read MMP block from device [%s]: %s\n",
				dev, error_message(retval));
			return CSM_AGAIN;
		}

		if (seq != mmp->mmp_seq) {
			printf("%s%s\n", occupied_string, mmp->mmp_nodename);
			return CSM_OCCUPIED;
		}
	}

	return CSM_MOUNTABLE;
}

static int
ldiskfs_check_mountable_openfs(const char *dev)
{
	errcode_t retval, err;
	ext2_filsys fs;

	retval = ext2fs_open(dev, LDISKFS_OPENFS_FLAGS, 0, 0,
			     unix_io_manager, &fs);
	if (retval) {
		fprintf(stderr, "unable to open fs on device [%s]\n",
			dev);
		return CSM_AGAIN;
	}

	retval = ldiskfs_check_mountable(dev, fs);

	err = ext2fs_close_free(&fs);
	if (err) {
		fprintf(stderr, "failed to close filesystem on device [%s]\n",
			dev);
	}
	return retval;
}

enum clf_device_type {
	CDT_UNKNOWN,
	CDT_EXT4,
	CDT_ZPOOL,
	CDT_LAST
};

static enum clf_device_type detect_device_type(char *dev)
{
	ext2_filsys fs;
	int rc;
	enum clf_device_type fs_type = CDT_UNKNOWN;

	rc = ext2fs_open(dev, LDISKFS_OPENFS_FLAGS,
			 0, 0, unix_io_manager, &fs);
	if (rc == 0) {
		fs_type = CDT_EXT4;
		ext2fs_close_free(&fs);
#ifdef HAVE_ZFS
	} else if (strchr(dev, '/') == NULL) {
		/*
		 * There isn't a good way to detect whether it is a zpool
		 * or not, but at least / is not allowed in zpool
		 */
		fs_type = CDT_ZPOOL;
#endif
	}
	return fs_type;
}

#ifdef HAVE_ZFS
static int zpools_check_mountable(char *poolname)
{
	int rc = CSM_MOUNTABLE;
	importargs_t idata = { 0 };
	nvlist_t *pools = NULL;
	nvpair_t *elem = NULL;
	nvlist_t *config = NULL;
	zpool_status_t zpool_status;
	zpool_errata_t errata;
	char *msgid;
	nvlist_t *nvinfo;
	//vdev_stat_t *vs;
	//uint_t vsc;
	//nvlist_t *nvroot, *nvinfo;
	int count = 0;
	zpool_handle_t *zhp;
	char *hostname;

	kernel_init(FREAD);
	g_zfs = libzfs_init();
	if (g_zfs == NULL) {
		fprintf(stderr, "failed tp init ZFS: %s",
			libzfs_error_init(errno));
		rc = CSM_AGAIN;
		goto out_kernel;
	}

	zhp = zpool_open_canfail(g_zfs, poolname);
	if (zhp != NULL) {
		fprintf(stderr, "zpool [%s] already imported\n",
			poolname);
		zpool_close(zhp);
		rc = CSM_EINVAL;
		goto out_fini;
	}

	idata.poolname = poolname;
	pools = zpool_search_import(g_zfs, &idata);
	if (pools == NULL) {
		fprintf(stderr, "no zpool with name [%s]\n",
			poolname);
		rc = CSM_EINVAL;
		goto out_fini;
	}

	while ((elem = nvlist_next_nvpair(pools, elem)) != NULL) {
		count++;
		if (count > 1) {
			fprintf(stderr, "multiple zpool with name [%s] found\n",
				poolname);
			rc = CSM_EINVAL;
			goto out_free;
		}
		verify(nvpair_value_nvlist(elem, &config) == 0);

		zpool_status = zpool_import_status(config, &msgid, &errata);
		switch (zpool_status) {
		case ZPOOL_STATUS_MISSING_DEV_R:
		case ZPOOL_STATUS_MISSING_DEV_NR:
		case ZPOOL_STATUS_BAD_GUID_SUM:
			fprintf(stderr,
				"one or more devices are missing from the system\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_CORRUPT_LABEL_R:
		case ZPOOL_STATUS_CORRUPT_LABEL_NR:
			fprintf(stderr,
				"one or more devices contains corrupted data\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_CORRUPT_DATA:
			fprintf(stderr,
				"the pool data is corrupted\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_OFFLINE_DEV:
			fprintf(stderr,
				"one or more devices are offline\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_CORRUPT_POOL:
			fprintf(stderr,
				"the pool metadata is corrupted\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_VERSION_OLDER:
			fprintf(stderr,
				"the pool is formatted using a legacy on-disk version\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_VERSION_NEWER:
			fprintf(stderr,
				"the pool is formatted using a incompatible version\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_FEAT_DISABLED:
			fprintf(stderr,
				"some supported features are not enabled on the pool\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_UNSUP_FEAT_READ:
			fprintf(stderr,
				"the pool uses feature(s) not supported on this system\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_UNSUP_FEAT_WRITE:
			fprintf(stderr,
				"the pool uses write feature(s) not supported on this system\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_HOSTID_ACTIVE:
			VERIFY0(nvlist_lookup_nvlist(config,
						     ZPOOL_CONFIG_LOAD_INFO,
						     &nvinfo));

			if (nvlist_exists(nvinfo, ZPOOL_CONFIG_MMP_HOSTNAME)) {
				hostname = fnvlist_lookup_string(nvinfo,
					ZPOOL_CONFIG_MMP_HOSTNAME);
				printf("%s%s\n", occupied_string, hostname);
			} else {
				printf("%s\n", occupied_string);
			}
			rc = CSM_OCCUPIED;
			goto out_free;
		case ZPOOL_STATUS_HOSTID_REQUIRED:
			fprintf(stderr,
				"the pool has the multihost property on\n");
			fprintf(stderr,
				"It cannot be safely imported when the system hostid is not set\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_HOSTID_MISMATCH:
			hostname = "another system";
			if (nvlist_exists(config, ZPOOL_CONFIG_HOSTNAME)) {
				hostname = fnvlist_lookup_string(config,
					ZPOOL_CONFIG_HOSTNAME);
			}
			fprintf(stderr,
				"the pool was last accessed by %s, import needs to have -f option\n",
				hostname);
			rc = CSM_FORCE_REQUIRED;
			goto out_free;
		case ZPOOL_STATUS_FAULTED_DEV_R:
		case ZPOOL_STATUS_FAULTED_DEV_NR:
			fprintf(stderr,
				"one or more devices are faulted\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_BAD_LOG:
			fprintf(stderr,
				"an intent log record cannot be read\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_RESILVERING:
			fprintf(stderr,
				"one or more devices were being resilvered\n");
			rc = CSM_FATAL;
			goto out_free;
		case ZPOOL_STATUS_ERRATA:
			fprintf(stderr,
				"errata #%d detected\n", errata);
			rc = CSM_FATAL;
			goto out_free;
		default:
			/*
			 * No other status can be seen when importing pools.
			 */
			assert(zpool_status == ZPOOL_STATUS_OK);
			rc = CSM_MOUNTABLE;
		}

		//verify(nvlist_lookup_nvlist(config, ZPOOL_CONFIG_VDEV_TREE,
		//      &nvroot) == 0);
		//verify(nvlist_lookup_uint64_array(nvroot,
		//				  ZPOOL_CONFIG_VDEV_STATS,
		//				  (uint64_t **)&vs, &vsc) == 0);
		//fprintf(stderr,
		//	"xxx vdev state %"PRId64
		//	", VDEV_STATE_HEALTHY %d, VDEV_STATE_DEGRADED %d VDEV_STATE_CANT_OPEN %d\n",
		//	vs->vs_state, VDEV_STATE_HEALTHY, VDEV_STATE_DEGRADED,
		//	VDEV_STATE_CANT_OPEN);
	}
	if (count == 0) {
		fprintf(stderr, "no zpool with name [%s] found\n",
			poolname);
		rc = CSM_EINVAL;
	}

out_free:
	nvlist_free(pools);
out_fini:
	libzfs_fini(g_zfs);
out_kernel:
	kernel_fini();
	return rc;
}
#endif

static void usage(char *prog)
{
	fprintf(stderr,
		"Usage: %s mountable <device|zpool_name>\n",
		prog);
}

int main(int argc, char **argv)
{
	int mountable;
	char *dev;
	enum clf_device_type device_type;

	if (argc != 3) {
		usage(argv[0]);
		return CSM_EINVAL;
	}
	if (strcmp(argv[1], "mountable") != 0) {
		usage(argv[0]);
		return CSM_EINVAL;
	}
	dev = argv[2];


	device_type = detect_device_type(dev);
	switch (device_type) {
	case CDT_EXT4:
		mountable = ldiskfs_check_mountable_openfs(dev);
		break;
#ifdef HAVE_ZFS
	case CDT_ZPOOL:
		mountable = zpools_check_mountable(dev);
		break;
#endif
	default:
		fprintf(stderr, "unknown fstype of device [%s]\n",
			dev);
		mountable = CSM_EINVAL;
	}
	return mountable;
}
