#include <stdio.h>
#include <stdlib.h>
#include <lustre/lustreapi.h>
#include <lustre/lustre_user.h>
#include "../ccoral/debug.h"

static void usage(char *prog)
{
	fprintf(stderr,
		"Usage: %s <service_name>\n",
		prog);
}

#define LUSTRE_SERVICE_TYPE_MGT "MGT"
#define LUSTRE_SERVICE_TYPE_MDT "MDT"
#define LUSTRE_SERVICE_TYPE_OST "OST"

static int lustre_parse_service_name(const char *service_name,
				     char *fsname,
				     int fsname_size,
				     char *type_name,
				     int type_name_size,
				     unsigned int *idx)
{
	/*
	 * "lustre0-MDT000a" -> "lustre0", "MDT", 10
	 * "lustre0-OST000a" -> "lustre0", "OST", 10
	 */
	int fsname_length;
	char *dash, *endp;
	const char *service_name_end = service_name + strlen(service_name);

	if (type_name_size <= 3) {
		CERROR("too short buffer for target type\n",
		       service_name);
		return -EINVAL;
	}

	/* format is "lustre-OST0001" */
	dash = memchr(service_name, '-', LUSTRE_MAXFSNAME + 1);
	if (!dash) {
		CERROR("invalid service name [%s] without dash\n",
		       service_name);
		return -EINVAL;
	}

	if (dash + 8 != service_name_end) {
		CERROR("invalid service name length [%s] after dash\n",
		       service_name);
		return -EINVAL;
	}

	fsname_length = dash - service_name;
	if (fsname_size <= fsname_length) {
		CERROR("too short buffer for fsname of [%s]\n",
		       service_name);
		return -EINVAL;
	}

	strncpy(fsname, service_name, fsname_length);
	fsname[fsname_length] = '\0';

	strncpy(type_name, dash + 1, 3);
	type_name[3] = '\0';
	if (strcmp(type_name, LUSTRE_SERVICE_TYPE_MDT) &&
	    strcmp(type_name, LUSTRE_SERVICE_TYPE_OST)) {
		CERROR("invalid service name [%s] with wrong type [%s]\n",
		       service_name, type_name);
		return -EINVAL;
	}

	dash += 4;
	*idx = strtoul(dash, &endp, 16);
	if (*idx > 0xffff || endp != service_name_end) {
		CERROR("invalid service name [%s] with wrong index [%s]\n",
		       service_name, dash);
		return -ERANGE;
	}

	return 0;
}

static inline int obd_statfs_ratio(const struct obd_statfs *st, bool inodes)
{
	double avail, used, ratio = 0;

	if (inodes) {
		avail = st->os_ffree;
		used = st->os_files - st->os_ffree;
	} else {
		avail = st->os_bavail;
		used = st->os_blocks - st->os_bfree;
	}
	if (avail + used > 0)
		ratio = used / (used + avail) * 100;

	/* Round up to match df(1) usage percentage */
	return (ratio - (int)ratio) > 0 ? (int)(ratio + 1) : (int)ratio;
}


int main(int argc, char **argv)
{
	int rc;
	__u32 type;
	__u32 index = 0;
	struct obd_statfs stat_buf;
	struct obd_uuid uuid_buf;
	const char *service_name;
	char fsname[LUSTRE_MAXFSNAME + 1];
	char type_name[4];
	char mnt_dir[PATH_MAX];
	__u64 total;
	__u64 used;
	__u64 available;

	if (argc != 2) {
		usage(argv[0]);
		return -EINVAL;
	}

	service_name = argv[1];
	rc = lustre_parse_service_name(service_name,
				       fsname, sizeof(fsname),
				       type_name, sizeof(type_name),
				       &index);
	if (rc)
		return rc;

	rc = llapi_search_rootpath(mnt_dir, fsname);
	if (rc < 0) {
		CERROR("failed to search mount point for file system [%s]: %s\n",
		       fsname, strerror(-rc));
		return rc;
	}

	if (strcmp(type_name, LUSTRE_SERVICE_TYPE_OST) == 0)
		type = LL_STATFS_LOV;
	else
		type = LL_STATFS_LMV;

	type |= LL_STATFS_NODELAY;
	rc = llapi_obd_statfs(mnt_dir, type, index, &stat_buf, &uuid_buf);
	if (rc) {
		CERROR("failed to stat [%s]: %s\n", service_name,
		       strerror(-rc));
		return rc;
	}

	total = stat_buf.os_blocks * stat_buf.os_bsize;
	available = stat_buf.os_bavail * stat_buf.os_bsize;
	used = total - stat_buf.os_bfree * stat_buf.os_bsize;

	fprintf(stdout, "total bytes: %llu\n", total);
	fprintf(stdout, "available bytes: %llu\n", available);
	fprintf(stdout, "used bytes: %llu\n", used);

	fprintf(stdout, "total 1K-blocks: %llu\n", total / 1024);
	fprintf(stdout, "available 1K-blocks: %llu\n", available / 1024);
	fprintf(stdout, "used 1K-blocks: %llu\n", used / 1024);
	fprintf(stdout, "used bytes ratio: %d%%\n",
		obd_statfs_ratio(&stat_buf, false));

	fprintf(stdout, "total inodes: %llu\n", stat_buf.os_files);
	fprintf(stdout, "available inodes: %llu\n", stat_buf.os_ffree);
	fprintf(stdout, "used inodes: %llu\n",
		stat_buf.os_files - stat_buf.os_ffree);
	fprintf(stdout, "used inodes ratio: %d%%\n",
		obd_statfs_ratio(&stat_buf, true));
	return 0;
}
