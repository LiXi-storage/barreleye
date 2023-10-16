/*
 *
 * General library for debugging
 *
 */

#ifndef _CORAL_DEBUG_H_
#define _CORAL_DEBUG_H_

#include <errno.h>
#include <stdio.h>
#include <stdarg.h>
#include <assert.h>
#include <stdbool.h>

enum debug_level {
	ERROR	= 0,
	WARN,
	INFO,
	DEBUG,
};

extern int debug_level;
extern int debug_level;
extern FILE *debug_log;
extern FILE *info_log;
extern FILE *error_log;

/* Print debug information. This is controlled by the value of the
 * global variable 'debug'
 */
static inline void _coral_logging(int level, bool ignore_level,
				  const char *fmt, ...)
{
	va_list ap;

	if (level > debug_level && !ignore_level)
		return;

	if (debug_log != NULL) {
		va_start(ap, fmt);
		vfprintf(debug_log, fmt, ap);
		va_end(ap);
		fflush(debug_log);
	}

	if (info_log != NULL && level <= INFO) {
		va_start(ap, fmt);
		vfprintf(info_log, fmt, ap);
		va_end(ap);
		fflush(info_log);
	}

	if (error_log != NULL && level <= ERROR) {
		va_start(ap, fmt);
		vfprintf(error_log, fmt, ap);
		va_end(ap);
		fflush(error_log);
	}

	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	fflush(stderr);
}

#define coral_logging_full(level, ignore_level, format, args...) \
	_coral_logging(level, ignore_level, \
		       "["#level"] [%s:%d] [%s()]: " format, \
		       __FILE__,  __LINE__, __func__, ##args)

#define coral_logging(level, ignore_level, format, args...) \
	_coral_logging(level, ignore_level, #level": " format, ##args)

#define CDEBUGL(level, format, args...) \
	coral_logging(level, false, format, ##args)

#define CERROR(format, args...) \
	coral_logging(ERROR, false, format, ##args)

#define CWARN(format, args...) \
	coral_logging(WARN, false, format, ##args)

#define CDEBUG(format, args...) \
	coral_logging(DEBUG, false, format, ##args)

#define CINFO(format, args...) \
	coral_logging(INFO, false, format, ##args)

typedef long long_ptr_t;

#define CORAL_ALLOC(ptr, size)	((ptr) = malloc(size))
#define CORAL_ALLOC_PTR(ptr) CORAL_ALLOC(ptr, sizeof *(ptr))

#define CORAL_FREE(ptr, size)	free(ptr)
#define CORAL_FREE_PTR(ptr) CORAL_FREE(ptr, sizeof *(ptr))

#define CBUG() assert(0)
#define CASSERT(e) assert(e)

#ifndef offsetof
# define offsetof(typ, memb) ((unsigned long)((char *)&(((typ *)0)->memb)))
#endif

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(a) ((sizeof(a)) / (sizeof((a)[0])))
#endif

#define BAD_USAGE(fmt, args...)						\
	do {								\
		CERROR("%s: " fmt					\
			"try '%s -h' for more information\n",		\
			program_invocation_short_name, ##args,		\
			program_invocation_short_name);			\
		exit(EXIT_FAILURE + 1);					\
	} while (0)
#endif /* _CORAL_DEBUG_H_ */
