#include <libgen.h>
#include <limits.h>
#include <mach-o/dyld.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;

    char exe_buf[PATH_MAX];
    uint32_t size = sizeof(exe_buf);
    char resolved_exe[PATH_MAX];
    char script_path[PATH_MAX];

    if (_NSGetExecutablePath(exe_buf, &size) == 0 && realpath(exe_buf, resolved_exe) != NULL) {
        char bin_dir[PATH_MAX];
        strncpy(bin_dir, resolved_exe, sizeof(bin_dir) - 1);
        bin_dir[sizeof(bin_dir) - 1] = '\0';
        char *dir_part = dirname(bin_dir);
        snprintf(script_path, sizeof(script_path), "%s/../dsh_mcp_stdio.py", dir_part);
    } else {
        snprintf(script_path, sizeof(script_path), "./dsh_mcp_stdio.py");
    }

    char *new_argv[4];
    new_argv[0] = "/usr/bin/python3";
    new_argv[1] = "-u";
    new_argv[2] = script_path;
    new_argv[3] = NULL;
    execv("/usr/bin/python3", new_argv);
    perror("execv /usr/bin/python3 failed");
    return 1;
}
