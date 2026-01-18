#include <gflags/gflags.h>

DEFINE_string(log_level, "info", "Minimum log level (trace, debug, info, warn, error, critical, off)");
DEFINE_string(log_file, "sr_engine.log", "Log file path");
DEFINE_int32(log_max_size, 10485760, "Max log file size in bytes before rotation");
DEFINE_int32(log_max_files, 3, "Number of rotated log files to keep");
