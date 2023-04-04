#!/bin/bash

# Upstream version of this script is in Erik Husby's 'shell-utils' repo:
# https://github.com/ehusby/shell-utils/blob/main/linux/exec/get_runtime_stats

## Bash settings
set -uo pipefail

## Script globals
script_name=$(basename "${BASH_SOURCE[0]}")
script_dir=$({ cd "$(dirname "${BASH_SOURCE[0]}")" || { echo "Failed to access script file directory" >&2; exit; } } && pwd)
script_dir_abs=$({ cd "$(dirname "${BASH_SOURCE[0]}")" || { echo "Failed to access script file directory" >&2; exit; } } && pwd -P)
script_file="${script_dir}/${script_name}"
if [ -L "${BASH_SOURCE[0]}" ]; then
    script_file_abs=$(readlink "${BASH_SOURCE[0]}")
else
    script_file_abs="${script_dir_abs}/${script_name}"
fi
export CURRENT_PARENT_BASH_SCRIPT_FILE="$script_file"
script_args=("$@")

### Script imports
#lib_dir="${script_dir}/../shell-utils/linux_bash/lib"
#bash_functions_script="${lib_dir}/bash_script_func.sh"
#
### Source imports
#source "$bash_functions_script"

####################
# Copied in the necessary bash function imports that would normally be sourced from shell-utils
# in order to make this script standalone.

print_string() { printf '%s' "$*"; }

echo_e()  { echo "$@" >&2; }
echo_oe() { echo "$@" | tee >(cat >&2); }

exit_script_with_status() {
    local status="$1"
    local script_file="$CURRENT_PARENT_BASH_SCRIPT_FILE"

    echo_e -e "\nError executing bash script, exiting with status code (${status}): ${script_file}"

    exit $status
}

re_test() {
    local re_test="$1"
    local test_str="$2"

    if [[ $test_str =~ $re_test ]]; then
        echo true
    else
        echo false
    fi
}
escape_regex_special_chars() {
    local special_chars_arr=( '^' '.' '+' '*' '?' '|' '/' '\\' '(' ')' '[' ']' '{' '}' '$' )
    local str_in="$1"
    local str_out=''
    local i char
    for (( i=0; i<${#str_in}; i++ )); do
        char="${str_in:$i:1}"
        if [ "$(itemOneOf "$char" "${special_chars_arr[@]}")" = true ]; then
            char="\\${char}"
        fi
        str_out="${str_out}${char}"
    done
    echo "$str_out"
}

string_startswith() { re_test "^$(escape_regex_special_chars "$2")" "$1"; }
string_endswith() { re_test "$(escape_regex_special_chars "$2")\$" "$1"; }
string_contains() { re_test "$(escape_regex_special_chars "$2")" "$1"; }

string_lstrip() {
    local string_in="$1"
    local strip_substr=''
    local string_stripped=''

    if (( $# >= 2 )) && [ -n "$2" ]; then
        strip_substr="$(escape_regex_special_chars "$2")"
    else
        strip_substr='[[:space:]]'
    fi

    string_stripped=$(print_string "$string_in" | sed -r "s/^($(print_string "$strip_substr"))+//")

    print_string "$string_stripped"
}

string_join() { local IFS="$1"; shift; print_string "$*"; }

#indexOf() { local el="$1"; shift; local arr=("$@"); local index=-1; local i; for i in "${!arr[@]}"; do [ "${arr[$i]}" = "$el" ] && { index="$i"; break; } done; echo "$index"; }
indexOf() {
    local el="$1"     # Save first argument in a variable
    shift             # Shift all arguments to the left (original $1 gets lost)
    local arr=("$@")  # Rebuild the array with rest of arguments
    local index=-1

    local i
    for i in "${!arr[@]}"; do
        if [ "${arr[$i]}" = "$el" ]; then
            index="$i"
            break
        fi
    done

    echo "$index"
}
#itemOneOf() { local el="$1"; shift; local arr=("$@"); if (( $(indexOf "$el" ${arr[@]+"${arr[@]}"}) == -1 )); then echo false; else echo true; fi }
itemOneOf() {
    local el="$1"
    shift
    local arr=("$@")

    if (( $(indexOf "$el" ${arr[@]+"${arr[@]}"}) == -1 )); then
        echo false
    else
        echo true
    fi
}

hms2sec() {
    local hms_str day_part hms_part
    local hms_hr hms_min hms_sec
    local total_sec

    hms_str=$(print_string "$1" | grep -Eo -m1 '[0-9]*-?[0-9]+:[0-9]{2}:[0-9]{2}')
    if [ -z "$hms_str" ]; then
        echo_e "hms2sec: unable to parse input string: ${1}"
        return 1
    fi

    IFS=- read -r day_part hms_part <<< "$hms_str"
    if [ -z "$hms_part" ]; then
        hms_part="$day_part"
        day_part=0
    fi

    IFS=: read -r hms_hr hms_min hms_sec <<< "${hms_part%.*}"
    total_sec="$(( 10#$day_part*86400 + 10#$hms_hr*3600 + 10#$hms_min*60 + 10#$hms_sec ))"

    echo "$total_sec"
}

####################


# A couple custom logging info prints have been added in these versions of get_stats from
# https://github.com/ehusby/shell-utils/blob/main/linux/lib/bash_shell_func.sh
get_stats() {
    # Adapted from https://stackoverflow.com/a/9790056/8896374
    local perl_cmd
    perl_cmd=''\
'use List::Util qw(max min sum);'\
'@num_list=(); while(<>){ $sqsum+=$_*$_; push(@num_list,$_); };'\
'$nitems=@num_list;'\
'if ($nitems == 0) { $sum=0; $min=0; $max=0; $med=0; $avg=0; $std=0; } else {'\
'$min=min(@num_list)+0; $max=max(@num_list)+0; $sum=sum(@num_list); $avg=$sum/$nitems;'\
'$std=sqrt($sqsum/$nitems-($sum/$nitems)*($sum/$nitems));'\
'$mid=int $nitems/2; @srtd=sort @num_list; if($nitems%2){ $med=$srtd[$mid]+0; }else{ $med=($srtd[$mid-1]+$srtd[$mid])/2; }; };'\
'print "\n"; print "\nRuntimes are reported in minutes\n\n";'\
'print "cnt: ${nitems}\nsum: ${sum}\nmin: ${min}\nmax: ${max}\nmed: ${med}\navg: ${avg}\nstd: ${std}\n";'\
'if ($nitems == 0) { exit(1); } else { exit(0); };'
    perl -e "$perl_cmd"
}
get_stats_plus_ref() {
    # Adapted from https://stackoverflow.com/a/9790056/8896374
    local perl_cmd
    perl_cmd=''\
'use List::Util qw(max min sum reduce);'\
'@file_list=(); @num_list=();'\
'while(<>){ @spl=split(",",$_); $file=$spl[0]; $num=$spl[1]; push(@file_list,$file); push(@num_list,$num); $sqsum+=$num*$num; };'\
'$nitems=@num_list;'\
'if ($nitems == 0) { $sum=0; $min=0; $max=0; $med=0; $avg=0; $std=0; } else {'\
'$min=min(@num_list)+0; $max=max(@num_list)+0; $sum=sum(@num_list); $avg=$sum/$nitems;'\
'$std=sqrt($sqsum/$nitems-($sum/$nitems)*($sum/$nitems));'\
'$mid_idx=int $nitems/2; @srtd=sort @num_list; if($nitems%2){ $med=$srtd[$mid_idx]+0; }else{ $med=($srtd[$mid_idx-1]+$srtd[$mid_idx])/2; }; };'\
'$mid_file=$file_list[$mid_idx];'\
'$min_idx=reduce { $num_list[$a] < $num_list[$b] ? $a : $b } 0..$#num_list; $min_file=$file_list[$min_idx];'\
'$max_idx=reduce { $num_list[$a] > $num_list[$b] ? $a : $b } 0..$#num_list; $max_file=$file_list[$max_idx];'\
'print "\n"; print "\nRuntimes are reported in minutes\n\n";'\
'print "cnt: ${nitems}\nsum: ${sum}\nmin: ${min} (${min_file})\nmax: ${max} (${max_file})\nmed: ${med} (${mid_file})\navg: ${avg}\nstd: ${std}\n";'\
'if ($nitems == 0) { exit(1); } else { exit(0); };'
    perl -e "$perl_cmd"
}


## Arguments
log_path_arr=()
logfname_patt_arr=( "qsub_*.sh.out" )
logfname_patt_provided=false
find_args=''
mode_choices=( 'timestamp' 'timestamp-filemod' 'runtime' )
mode='runtime'
#timestamp_grep="([A-Z][a-z]+ +[A-Z][a-z]+ +[0-9]+ +[0-9]{2}:[0-9]{2}:[0-9]{2} +[A-Z]+ +[0-9]+)"  # 'date' program default format
#timestamp_sed="s|^||"  # sed expression to not change anything
timestamp_grep="([0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2})"  # "month-day-year HH:MM:SS"
timestamp_sed="s|([0-9]{2})-([0-9]{2})-([0-9]{4})|\3-\1-\2|"  # change "month-day-year" to "year-month-day"
filemod_grep="^Modify: ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"
#runtime_grep="^real[[:space:]]+([0-9]+m[0-9]+\.[0-9]+s)"  # 'time' program default format
#runtime_sed="s|^([0-9]+)m([0-9]+\.[0-9]+)s$|\1 * 60 + \2|"  # convert 'time' program values to seconds
runtime_grep="Total Processing Time: ([0-9]+:[0-9]{2}:[0-9]{2})"  # reported in (H)H:MM:SS
runtime_sed="s|^||"  # sed expression to not change anything
runtime_is_hms_choices=( 'true' 'false' )
runtime_is_hms=false
runtime_match_choices=( 'first' 'last' 'all' )
runtime_match='all'
runtime_report_choices=( 'separate' 'sum' )
runtime_report='sum'
count_first_choices=( 'on' 'off' )
count_first='on'
include_ref_choices=( 'on' 'off' )
include_ref='on'
max_files=''


## Script usage
read -r -d '' script_usage << EOM
Usage: ${script_name} [OPTION] LOG_PATH...

  Calculate and print runtime statistics sourced from all
process log files found within one or more directories (or files).

  Log files are located using a (recursive) 'find' search on
all input LOG_PATH (most commonly directories, but may be paths
to log files) for files with filenames that match the provided
--logfname-pattern string(s).

  In 'timestamp' mode, runtime for each log file is determined
by differencing the times of the first and last reported timestamp
strings found in the log file. These timestamp strings are found
using a 'grep' match against the provided --timestamp-grep pattern
string.
The parsed timestamps are converted to seconds using the standard
POSIX 'date' program with the function 'date -d "timestamp" +%s'.
This function accepts timestamps in the following example formats:
  Sun Jan 2 23:45:01 CST 2022
  2022-01-02 23:45:01
The --timestamp-grep pattern may need to be adjusted from its
default setting to support the timestamp format found in the
particular log files to be parsed. If the log file timestamp
format does not match one of the formats accepted by 'date -d',
the --timestamp-sed option should be used to manipulate the
timestamp strings into one of the accepted formats using the
provided 'sed' expression.

  In 'timestamp-filemod' mode, runtime for each log file is
determined by differencing the time of the first reported timestamp
found in the log file with the last modified time of the log file
itself as reported by 'stat LOGFILE_PATH'.
The --filemod-grep pattern may need to be adjusted from its
default setting to correctly parse the "Modify" time printed by the
'stat' program.

  In 'runtime' mode, runtime is scraped from one or more log message
strings reported within the log files themselves. These strings are
found using a 'grep' match against the provided --runtime-grep
pattern string.
If the parsed runtime strings are in hours-minutes-seconds (HMS)
format (i.e. '1:23:45' represents a runtime of 1 hour, 23 minutes,
and 45 seconds), then set --runtime-is-hms='true' and the
--runtime-sed option should be used as necessary to convert the
reported runtime into a pure '(H)H:MM:SS' format.
If the parsed runtime strings are not in HMS format, then set
--runtime-is-hms='false'. The --runtime-sed option should be used
to convert the runtime string into an expression that, when run
through the standard POSIX 'bc' program, results in a single number
printed out that is equal to the runtime in seconds.

Options:
 -p,--logfname-pattern (default='${logfname_patt_arr[*]}')
        'find' program '-name' pattern string used to recursively
        locate all log files to be processed within all LOG_PATH
        (directories).
        Can be provided multiple times to process files that match
        *at least one* of the provided patterns.
   --find-args
        A single string of optional arguments to pass to the 'find'
        command used to recursively locate all log files to be
        processed within all LOG_PATH (directories).
   --mode={$(string_join '|' "${mode_choices[@]}")} (default=${mode})
        Whether the program should operate in 'timestamp' or 'runtime'
        mode, based on what information can be parsed from the source
        log files.
   --timestamp-grep (default='${timestamp_grep}')
        Timestamp pattern string given to 'grep -Eo' to parse the
        contents of log files for the first and last recorded
        timestamps.
        The pattern can include text before and/or after the core
        timestamp pattern that helps uniquely identify the desired
        timestamps to be be parsed. For this reason, the timestamp
        portion of the pattern string MUST BE WRAPPED IN PARENTHESES ()
        in order to identify it as capture group 1 (\\1) in a 'sed'
        process that follows to filter out the extra text.
   --timestamp-sed (default='${timestamp_sed}')
        'sed -r' expression used to manipulate parsed timestamps from
        their original format into a format accepted by 'date -d'.
   --filemod-grep (default='${filemod_grep}')
        Pattern string given to 'grep -Eo' to parse the "Modify" time
        of log files as printed by 'stat LOGFILE_PATH'.
        The pattern can include text before and/or after the core
        timestamp pattern that helps uniquely identify the desired
        timestamps to be be parsed. For this reason, the timestamp
        portion of the pattern string MUST BE WRAPPED IN PARENTHESES ()
        in order to identify it as capture group 1 (\\1) in a 'sed'
        process that follows to filter out the extra text.
    --runtime-grep (default='${runtime_grep}')
        Runtime pattern string given to 'grep -Eo' to parse the
        contents of log files for reported runtime(s).
        The pattern can include text before and/or after the core
        timestamp pattern that helps uniquely identify the desired
        timestamps to be be parsed. For this reason, the timestamp
        portion of the pattern string MUST BE WRAPPED IN PARENTHESES ()
        in order to identify it as capture group 1 (\\1) in a 'sed'
        process that follows to filter out the extra text.
    --runtime-sed (default='${runtime_sed}')
        'sed -r' expression used to manipulate parsed runtimes from
        their original format into either:
        (--runtime-is-hms='true')
            the pure hours-minutes-seconds format, '(H)H:MM:SS'.
        (--runtime-is-hms='false')
            an expression that, when run through the 'bc' program,
            results in a single number printed out that is equal to
            the runtime in seconds.
    --runtime-is-hms={$(string_join '|' "${runtime_is_hms_choices[@]}")} (default=${runtime_is_hms})
        Inform the parser in 'runtime' mode whether runtime strings
        in the source log files are in hours-minutes-seconds format.
    --runtime-match={$(string_join '|' "${runtime_match_choices[@]}")} (default=${runtime_match})
        In 'runtime' mode, whether the first, last, or all runtimes
        parsed from source log files should be considered.
    --runtime-report={$(string_join '|' "${runtime_report_choices[@]}")} (default=${runtime_report})
        In 'runtime' mode with --runtime-match='all', whether the case
        of multiple runtimes parsed from a single source log file
        should be reported as statistically separate runtimes or
        summed together and reported as a single combined runtime.
-cf,--count-first (default='${count_first}')
        Count the total number of log files in all LOG_PATH
        (directories) that match --logfname-pattern so that this
        number can be displayed in a progress bar during the
        subsequent step of parsing timestamps from those files.
-ir,--include-ref (default='${include_ref}')
        Include reference logfile path next to min, max, and med
        statistics in printed stats information.
-mf,--max-files
        Specify the maximum number of logfiles that are processed
        in each LOG_PATH directory.
        If not provided, all logfiles found within all LOG_PATH
        directories are processed.
EOM
if (( $# < 1 )); then
    echo_e -e "$script_usage"
    exit_script_with_status 1
fi


## Parse arguments
set +u
while (( $# > 0 )); do
    arg="$1"

     if [ "$(string_startswith "$arg" '-')" = false ]; then
        log_path_arr+=( "$arg" )

    else
        arg_opt="$(string_lstrip "$arg" '-')"
        arg_opt_nargs=''
        if [ "$(string_contains "$arg_opt" '=')" = true ]; then
            arg_val=$(printf '%s' "${arg_opt#*=}" | sed -r -e "s|^['\"]+||" -e "s|['\"]+$||")
            arg_opt="${arg_opt%%=*}"
            arg_opt_nargs_do_shift=false
        else
            arg_val="$2"
            arg_opt_nargs_do_shift=true
        fi
        arg_val_can_start_with_dash=false

        if [ "$arg_opt" = 'h' ] || [ "$arg_opt" = 'help' ]; then
            arg_opt_nargs=0
            echo "$script_usage"
            exit 0

        elif [ "$arg_opt" = 'p' ] || [ "$arg_opt" = 'logfname-pattern' ]; then
            arg_opt_nargs=1
            if [ "$logfname_patt_provided" = false ]; then
                logfname_patt_provided=true
                logfname_patt_arr=()
            fi
            if [ -n "$arg_val" ]; then
                logfname_patt_arr+=( "$arg_val" )
            fi

        elif [ "$arg_opt" = 'find-args' ]; then
            arg_opt_nargs=1
            find_args="$arg_val"

        elif [ "$arg_opt" = 'mode' ]; then
            arg_opt_nargs=1
            mode="$arg_val"

        elif [ "$arg_opt" = 'timestamp-grep' ]; then
            arg_opt_nargs=1
            timestamp_grep="$arg_val"

        elif [ "$arg_opt" = 'timestamp-sed' ]; then
            arg_opt_nargs=1
            timestamp_sed="$arg_val"

        elif [ "$arg_opt" = 'filemod-grep' ]; then
            arg_opt_nargs=1
            runtime_grep="$arg_val"

        elif [ "$arg_opt" = 'runtime-grep' ]; then
            arg_opt_nargs=1
            runtime_grep="$arg_val"

        elif [ "$arg_opt" = 'runtime-sed' ]; then
            arg_opt_nargs=1
            runtime_sed="$arg_val"

        elif [ "$arg_opt" = 'runtime-is-hms' ]; then
            arg_opt_nargs=1
            runtime_is_hms="$arg_val"

        elif [ "$arg_opt" = 'runtime-match' ]; then
            arg_opt_nargs=1
            runtime_match="$arg_val"

        elif [ "$arg_opt" = 'runtime-report' ]; then
            arg_opt_nargs=1
            runtime_report="$arg_val"

        elif [ "$arg_opt" = 'cf' ] || [ "$arg_opt" = 'count-first' ]; then
            arg_opt_nargs=1
            count_first="$arg_val"

        elif [ "$arg_opt" = 'ir' ] || [ "$arg_opt" = 'include-ref' ]; then
            arg_opt_nargs=1
            include_ref="$arg_val"

        elif [ "$arg_opt" = 'mf' ] || [ "$arg_opt" = 'max-files' ]; then
            arg_opt_nargs=1
            max_files="$arg_val"

        else
            echo_e "Unexpected argument: ${arg}"
            exit_script_with_status 1
        fi

        if [ -z "$arg_opt_nargs" ]; then
            echo_e "Developer error! "'$arg_opt_nargs'" was not set for argument: ${arg}"
            exit_script_with_status 1
        fi

        if [ "$arg_opt_nargs_do_shift" = true ] && (( arg_opt_nargs >= 1 )); then
            for arg_num in $(seq 1 $arg_opt_nargs); do
                shift
                arg_val="$1"
                if [ -z "$arg_val" ]; then
                    echo_e "Missing expected value (#${arg_num}) for argument: ${arg}"
                    exit_script_with_status 1
                elif [ "$arg_val_can_start_with_dash" = false ] && [ "$(string_startswith "$arg_val" '-')" = true ]; then
                    echo_e "Unexpected argument value: ${arg} ${arg_val}"
                    exit_script_with_status 1
                fi
            done
        fi
    fi

    shift
done
set -u


## Validate arguments

if (( ${#log_path_arr[@]} == 0 )); then
    echo_e "At least one LOG_PATH path must be provided"
    exit_script_with_status 1
fi
dir_among_log_paths=false
for log_path in "${log_path_arr[@]}"; do
    if [ -d "$log_path" ]; then
        dir_among_log_paths=true
    elif [ ! -e "$log_path" ]; then
        echo_e "LOG_PATH path does not exist: ${log_path}"
        exit_script_with_status 1
    fi
done
if [ "$(itemOneOf "$mode" "${mode_choices[@]}")" = false ]; then
    echo_e "--mode setting must be one of the following: ${mode_choices[*]}"
    exit_script_with_status 1
fi
if [ "$(itemOneOf "$runtime_is_hms" "${runtime_is_hms_choices[@]}")" = false ]; then
    echo_e "--runtime-is-hms setting must be one of the following: ${runtime_is_hms_choices[*]}"
    exit_script_with_status 1
fi
if [ "$(itemOneOf "$runtime_match" "${runtime_match_choices[@]}")" = false ]; then
    echo_e "--runtime-match setting must be one of the following: ${runtime_match_choices[*]}"
    exit_script_with_status 1
fi
if [ "$(itemOneOf "$runtime_report" "${runtime_report_choices[@]}")" = false ]; then
    echo_e "--runtime-report setting must be one of the following: ${runtime_report_choices[*]}"
    exit_script_with_status 1
fi
if [ "$(itemOneOf "$count_first" "${count_first_choices[@]}")" = false ]; then
    echo_e "--count-first setting must be one of the following: ${count_first_choices[*]}"
    exit_script_with_status 1
fi
if [ "$(itemOneOf "$include_ref" "${include_ref_choices[@]}")" = false ]; then
    echo_e "--include-ref setting must be one of the following: ${include_ref_choices[*]}"
    exit_script_with_status 1
fi


# Build -name arguments to give to 'find' command
if [ "$dir_among_log_paths" = true ] && (( ${#logfname_patt_arr[@]} > 0 )); then
    logfname_patt_combined_str=$(printf " '%s'" "${logfname_patt_arr[@]}")
    logfname_patt_combined_str=$(string_lstrip "$logfname_patt_combined_str")
    find_name_args="\("
    for i in "${!logfname_patt_arr[@]}"; do
        if (( i == 0 )); then
            find_name_args="${find_name_args} -name '${logfname_patt_arr[i]}'"
        else
            find_name_args="${find_name_args} -o -name '${logfname_patt_arr[i]}'"
        fi
    done
    find_name_args="${find_name_args} \)"
else
    logfname_patt_combined_str="*"
    find_name_args=''
fi

if [ "$runtime_match" = 'first' ]; then
    runtime_grep_preprocess='cat'
    runtime_grep_match_count_arg="-m1"
elif [ "$runtime_match" = 'last' ]; then
    runtime_grep_preprocess='tac'
    runtime_grep_match_count_arg="-m1"
elif [ "$runtime_match" = 'all' ]; then
    runtime_grep_preprocess='cat'
    runtime_grep_match_count_arg=''
fi

if [ "$mode" = 'timestamp' ] || [ "$mode" = 'timestamp-filemod' ]; then
    time_string_pattern="$timestamp_grep"
elif [ "$mode" = 'runtime' ]; then
    time_string_pattern="$runtime_grep"
fi

if [ "$include_ref" = 'on' ]; then
    get_stats_fn='get_stats_plus_ref'
elif [ "$include_ref" = 'off' ]; then
    get_stats_fn='get_stats'
fi


## Process log files in all source log directories

if [ "$count_first" = 'on' ]; then
    echo "First counting log files in source LOG_PATHs..."
    echo "(set --count-first='off' to skip this step)"
    nlogfiles_total=0
    for log_path in "${log_path_arr[@]}"; do
        nlogfiles_path=$(eval find "$log_path" ${find_args} -type f "${find_name_args}" | wc -l)
        if [ -n "$max_files" ] && (( nlogfiles_path > max_files )); then
            nlogfiles_path="$max_files"
        fi
        nlogfiles_total=$((nlogfiles_total + nlogfiles_path))
    done
    echo
elif [ "$count_first" = 'off' ]; then
    nlogfiles_total='?'
fi

#matched_a_time_in_any_logfile=false

nlogfiles_format="%0${#nlogfiles_total}d"
nlogfiles_i=0
printf "Processing log files matching ${logfname_patt_combined_str}: (${nlogfiles_format}/${nlogfiles_total})\r" "$nlogfiles_i" >/dev/stderr
for log_path in "${log_path_arr[@]}"; do
    nlogfiles_i_path=0
    while IFS= read -r -d '' logfile; do
        if [ -z "$logfile" ]; then continue; fi
        ((nlogfiles_i++))
        ((nlogfiles_i_path++))
    #    echo -en "Processing log files matching ${logfname_patt_combined_str}: (${nlogfiles_i}/${nlogfiles_total})\r" >/dev/stderr
        printf "Processing log files matching ${logfname_patt_combined_str}: (${nlogfiles_format}/${nlogfiles_total})\r" "$nlogfiles_i" >/dev/stderr

        if [ "$mode" = 'timestamp' ] || [ "$mode" = 'timestamp-filemod' ]; then

            time_start_match=$(grep -Eo -m1 "$timestamp_grep" "$logfile")
            if (( $? != 0 )); then
                continue
            fi
            if [ "$mode" = 'timestamp' ]; then
                set +o pipefail
                time_end_match=$(tac "$logfile" | grep -Eo -m1 "$timestamp_grep")
                status=$?
                set -o pipefail
                if (( status != 0 )); then
                    continue
                fi
            fi

    #        if [ "$matched_a_time_in_any_logfile" = false ]; then
    #            matched_a_time_in_any_logfile=true
    #        fi

            time_start_datetime=$(printf '%s' "$time_start_match" | sed -r "s|${timestamp_grep}|\1|" | sed -r "$timestamp_sed")
            time_start_sec=$(date -d "$time_start_datetime" +%s)

            if [ "$mode" = 'timestamp' ]; then
                time_end_datetime=$(printf '%s' "$time_end_match" | sed -r "s|${timestamp_grep}|\1|" | sed -r "$timestamp_sed")
                time_end_sec=$(date -d "$time_end_datetime" +%s)
            elif [ "$mode" = 'timestamp-filemod' ]; then
                time_end_datetime=$(stat "$logfile" | grep -Eo -m1 "$filemod_grep" | sed -r "s|${filemod_grep}|\1|")
                time_end_sec=$(date -d "$time_end_datetime" +%s)
            fi

            runtime_sec="$((time_end_sec - time_start_sec))"
            runtime_min=$(echo "scale=3 ; ${runtime_sec} / 60" | bc)

            if [ "$get_stats_fn" = 'get_stats' ]; then
                echo "$runtime_min"
            elif [ "$get_stats_fn" = 'get_stats_plus_ref' ]; then
                echo "${logfile},${runtime_min}"
            fi

        elif [ "$mode" = 'runtime' ]; then
            runtime_min_total=0
            matched_a_runtime_in_this_logfile=false

            while IFS= read -r runtime_match; do
    #            if [ "$matched_a_time_in_any_logfile" = false ]; then
    #                matched_a_time_in_any_logfile=true
    #            fi
                if [ "$matched_a_runtime_in_this_logfile" = false ]; then
                    matched_a_runtime_in_this_logfile=true
                fi

                if [ "$runtime_is_hms" = true ]; then
                    runtime_sec=$(hms2sec "$runtime_match")
                    if (( $? != 0 )); then
                        echo_e "Function failure: hms2sec '${runtime_match}', ${logfile}"
                        exit_script_with_status 1
                    fi
                else
                    runtime_sec=$(echo "$runtime_match" | bc)
                    if (( $? != 0 )); then
                        echo_e "Function failure: echo '${runtime_match}' | bc, ${logfile}"
                        exit_script_with_status 1
                    fi
                fi

                bc_expr_sec2min="scale=3 ; ${runtime_sec} / 60"
                runtime_min=$(echo "$bc_expr_sec2min" | bc)
                if (( $? != 0 )); then
                    echo_e "Function failure: echo '${bc_expr_sec2min}' | bc, ${logfile}"
                    exit_script_with_status 1
                fi

                if [ "$runtime_report" = 'separate' ]; then
                    echo "$runtime_min"
                elif [ "$runtime_report" = 'sum' ]; then
                    bc_expr_sum="${runtime_min_total} + ${runtime_min}"
                    runtime_min_total=$(echo "$bc_expr_sum" | bc)
                    if (( $? != 0 )); then
                        echo_e "Function failure: echo '${bc_expr_sum}' | bc, ${logfile}"
                        exit_script_with_status 1
                    fi
                fi

            done < <(${runtime_grep_preprocess} "$logfile" | grep -Eo ${runtime_grep_match_count_arg} "$runtime_grep" | sed -r "s|${runtime_grep}|\1|" | sed -r "$runtime_sed")

            if [ "$runtime_report" = 'sum' ] && [ "$matched_a_runtime_in_this_logfile" = true ]; then
                if [ "$get_stats_fn" = 'get_stats' ]; then
                    echo "$runtime_min_total"
                elif [ "$get_stats_fn" = 'get_stats_plus_ref' ]; then
                    echo "${logfile},${runtime_min_total}"
                fi
            fi
        fi

        if [ -n "$max_files" ] && (( nlogfiles_i_path == max_files )); then
            break;
        fi

    done < <(eval find "$log_path" ${find_args} -type f "${find_name_args}" -print0 2>/dev/null)
done | ${get_stats_fn}

# Commented this out because these variables can't be updated
# when while loop output is piped to 'get_stats' function
# (the while loop is run in a subshell).
#if (( nlogfiles_i == 0 )); then
#    echo
#    echo_e "No files found in LOG_PATHs matching log filename pattern: '${find_name_args}'"
#    exit_script_with_status 1
#elif [ "$matched_a_time_in_any_logfile" = false ]; then
#    echo
#    echo_e "Could not find any string matches in all source log files for ${mode} pattern: '${time_string_pattern}'"
#    exit_script_with_status 1
#fi

if (( $? != 0 )); then
    echo
    if [ "$count_first" = 'on' ] && (( nlogfiles_total == 0 )); then
        echo_e "No files found in LOG_PATHs matching log filename pattern(s): \"${find_name_args}\""
    elif [ "$count_first" = 'off' ]; then
        echo_e "Failed to get runtime stats"
        echo_e "Please try again with --count-first='on' to further diagnose any issues"
    else
        echo_e "Either hit function failure, or could not find any string matches in all source log files for ${mode} pattern: \"${time_string_pattern}\""
    fi
    exit_script_with_status 1
fi
