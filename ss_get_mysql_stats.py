#!/usr/bin/python -w
# ============================================================================
# This script is based on ss_get_mysql_stats.php 
#  from Percona Monitoring Plugins available here:
#  http://www.percona.com/software/percona-monitoring-plugins
# License: GPL License (see COPYING)
# Copyright 2013 PalominoDB Inc.
# Authors:
#  Rod Xavier Bondoc
# ============================================================================
import argparse
import fcntl
import math
import os
import pprint
import re
import sys
import time
import traceback
from datetime import datetime
from socket import socket

import MySQLdb
import MySQLdb.cursors

mysql_user = 'statsuser'
mysql_pass = 'statsuserpass'
mysql_port = 3306

graphite_host = '127.0.0.1'
graphite_port = 2003

heartbeat  = ''            # db.tbl if you use pt-heartbeat from Percona Toolkit.
cache_dir  = '/tmp'        # If set, this uses caching to avoid multiple calls.
poll_time  = 300           # Adjust to match your polling interval.
timezone   = None          # If not set, uses the system default.  Example: "UTC"
chk_options = {
    'innodb': True,         # Do you want to check InnoDB statistics?
    'master': True,         # Do you want to check binary logging?
    'slave': True,          # Do you want to check slave status?
    'procs': True,          # Do you want to check SHOW PROCESSLIST?
    'get_qrt': True,        # Get query response times from Percona Server?
}

debug     = False          # Define whether you want debugging behavior.
debug_log = False          # If $debug_log is a filename, it'll be used.

# ============================================================================
# This is the main function.  Some parameters are filled in from defaults at the
# top of this file.
# ============================================================================
def ss_get_mysql_stats(options):
    # Process connection options and connect to MySQL.
    global cache_dir, poll_time, chk_options
        
    # Connect to MySQL.
    host = options.host
    user = options.user
    passwd = options.password
    port = options.port
    heartbeat = options.heartbeat
    
    db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, cursorclass=MySQLdb.cursors.DictCursor)
    cursor = db.cursor()
    
    sanitized_host = host.replace(':', '').replace('/', '_')
    sanitized_host = sanitized_host + '_' + str(port)
    cache_file = os.path.join(cache_dir, '%s-mysql_graphite_stats.txt' % (sanitized_host))
    log_debug('Cache file is %s' % (cache_file))
    
    # First, check the cache.
    fp = None
    if not options.nocache:
        with open(cache_file, 'a+') as fp:
            try:
                fcntl.flock(fp, fcntl.LOCK_SH) # LOCK_SH
                try:
                    lines = open(cache_file).readlines()
                except Exception:
                    lines = []
                if os.path.getsize(cache_file) > 0 and \
                        os.path.getctime(cache_file) + (poll_time/2) > int(time.time()) and \
                        len(lines) > 0:
                    # The cache file is good to use
                    log_debug('Using the cache file')
                    return lines[0]
                else:
                    log_debug('The cache file seems too small or stale')
                    try:
                        # Escalate the lock to exclusive, so we can write to it.
                        fcntl.flock(fp, fcntl.LOCK_EX) # LOCK_EX
                        try:
                            lines = open(cache_file).readlines()
                        except Exception:
                            lines = []
                        if os.path.getsize(cache_file) > 0 and \
                                os.path.getctime(cache_file) + (poll_time/2) > int(time.time()) and \
                                len(lines) > 0:
                            log_debug("Using the cache file")
                            return lines[0]
                        f.truncate(0)
                    except Exception:
                        pass
            except IOError:
                log_debug("Couldn't lock the cache file, ignoring it.")
                fp = None
    else:
        log_debug("Couldn't open cache file")
        fp = None
        
    # Set up variables
    status = { # Holds the result of SHOW STATUS, SHOW INNODB STATUS, etc
        # Define some indexes so they don't cause errors with += operations
        'relay_log_space'           : None,
        'binary_log_space'          : None,
        'current_transactions'      : 0,
        'locked_transactions'       : 0,
        'active_transactions'       : 0,
        'innodb_locked_tables'      : 0,
        'innodb_tables_in_use'      : 0,
        'innodb_lock_structs'       : 0,
        'innodb_lock_wait_secs'     : 0,
        'innodb_sem_waits'          : 0,
        'innodb_set_wait_time_ms'   : 0,
        # Values for the 'state' column from SHOW PROCESSLIST (converted to
        # lowercase, with spaces replaced by underscores)
        'State_closing_tables'      : None,
        'State_copying_to_tmp_table': None,
        'State_end'                 : None,
        'State_freeing_items'       : None,
        'State_init'                : None,
        'State_login'               : None,
        'State_preparing'           : None,
        'State_reading_from_net'    : None,
        'State_sending_data'        : None,
        'State_sorting_result'      : None,
        'State_statistics'          : None,
        'State_updating'            : None,
        'State_writing_to_net'      : None,
        'State_none'                : None,
        'State_other'               : None, # Everything not listed above
    }
    
    # Get SHOW STATUS
    cursor.execute("SHOW /*!50002 GLOBAL */ STATUS")
    result = cursor.fetchall()
    for row in result:
        row = dict_change_key_case(row, case='lower')
        status[row.get('variable_name')] = row.get('value')

    # Get SHOW VARIABLES
    cursor.execute('SHOW VARIABLES')
    result = cursor.fetchall()
    for row in result:
        row = dict_change_key_case(row, case='lower')
        status[row.get('variable_name')] = row.get('value')
     
    # Get SHOW SLAVE STATUS 
    if chk_options.get('slave'):
        cursor.execute('SHOW SLAVE STATUS')
        result = cursor.fetchall()
        slave_status_row_gotten = 0
        for row in result:
            slave_status_row_gotten += 1
            # Must lowercase keys because different MySQL versions have different
            # lettercase.
            row = dict_change_key_case(row, case='lower')
            status['relay_log_space'] = row.get('relay_log_space')
            status['slave_lag'] = row.get('seconds_behind_master')
            
            if len(heartbeat) > 0:
                cursor.execute(
                    'SELECT MAX(GREATEST(0, UNIX_TIMESTAMP() - UNIX_TIMESTAMP(ts) - 1)) AS delay FROM %s' % (heartbeat)
                )
                result2 = cursor.fetchall()
                slave_delay_rows_gotten = 0
                for row2 in result2:
                    slave_delay_rows_gotten += 1
                    if type(row2) == dict and 'delay' in row2.keys():
                        status['slave_lag'] = row2.get('delay')
                    else:
                        log_debug("Couldn't get slave lag from %s" % (heartbeat))
                        
                if slave_delay_rows_gotten == 0:
                    log_debug('Got nothing from heartbeat query')
            
            # Scale slave_running and slave_stopped relative to the slave lag.
            status['slave_running'] = status.get('slave_lag') if row.get('slave_sql_running') == 'Yes' else 0
            status['slave_stopped'] = 0 if row.get('slave_sql_running') == 'Yes' else status.get('slave_lag')
        
        if slave_status_row_gotten == 0:
            log_debug('Got nothing from SHOW SLAVE STATUS')
    
    # Get SHOW MASTER STATUS
    if chk_options.get('master') and status.get('log_bin') == 'ON':
        binlogs = []
        cursor.execute('SHOW MASTER LOGS')
        result = cursor.fetchall()
        for row in result:
            row = dict_change_key_case(row, case='lower')
            # Older versions of MySQL may not have the File_size column in the
            # results of the command.  Zero-size files indicate the user is
            # deleting binlogs manually from disk (bad user! bad!).
            if 'file_size' in row.keys() and row.get('file_size') > 0:
                binlogs.append(row.get('file_size'))
                
        if len(binlogs) > 0:
            status['binary_log_space'] = sum(binlogs)

    # Get SHOW PROCESSLIST and aggregate it by state
    if chk_options.get('procs'):
        cursor.execute('SHOW PROCESSLIST')
        result = cursor.fetchall()
        for row in result:
            state = row.get('state')
            if state is None:
                state = 'NULL'
            if state == '':
                state = 'none'
                
            # MySQL 5.5 replaces the 'Locked' state with a variety of "Waiting for
            # X lock" types of statuses.  Wrap these all back into "Locked" because
            # we don't really care about the type of locking it is.
            state = re.sub('^(Table lock|Waiting for .*lock)$', 'Locked', state)
            state = state.replace(' ', '_')
            if 'State_%s' % (state) in status.keys():
                increment(status, 'State_%s' % (state), 1)
            else:
                increment(status, 'State_other', 1)

    # Get SHOW INNODB STATUS and extract the desired metrics from it
    if chk_options.get('innodb') and status.get('have_innodb') == 'YES':
        cursor.execute('SHOW /*!50000 ENGINE*/ INNODB STATUS')
        result = cursor.fetchall()
        istatus_text = result[0].get('Status')
        istatus_vals = get_innodb_array(istatus_text)
        
        if chk_options.get('get_qrt') and status.get('have_response_time_distribution') == 'YES':
            log_debug('Getting query time histogram')
            i = 0
            cursor.execute(
                '''
                SELECT `count`, total * 1000000 AS total
                FROM INFORMATION_SCHEMA.QUERY_RESPONSE_TIME
                WHERE `time` <> 'TOO LONG'
                '''
            )
            result = cursor.fetchall()
            for row in result:
                if i > 13:
                    # It's possible that the number of rows returned isn't 14.
                    # Don't add extra status counters.
                    break
                count_key = 'Query_time_count_%02d' % (i)
                total_key = 'Query_time_total_%02d' % (i)
                status[count_key] = row['count']
                status[total_key] = row['total']
                i += 1
            # It's also possible that the number of rows returned is too few.
            # Don't leave any status counters unassigned; it will break graphs.
            while i <= 13:
                count_key = 'Query_time_count_%02d' % (i)
                total_key = 'Query_time_total_%02d' % (i)
                status[count_key] = 0
                status[total_key] = 0
                i += 1
        else:
            log_debug('Not getting time histogram because it is not enabled')        
            
        # Override values from InnoDB parsing with values from SHOW STATUS,
        # because InnoDB status might not have everything and the SHOW STATUS is
        # to be preferred where possible.
        
        overrides = {
            'Innodb_buffer_pool_pages_data'  : 'database_pages',
            'Innodb_buffer_pool_pages_dirty' : 'modified_pages',
            'Innodb_buffer_pool_pages_free'  : 'free_pages',
            'Innodb_buffer_pool_pages_total' : 'pool_size',
            'Innodb_data_fsyncs'             : 'file_fsyncs',
            'Innodb_data_pending_reads'      : 'pending_normal_aio_reads',
            'Innodb_data_pending_writes'     : 'pending_normal_aio_writes',
            'Innodb_os_log_pending_fsyncs'   : 'pending_log_flushes',
            'Innodb_pages_created'           : 'pages_created',
            'Innodb_pages_read'              : 'pages_read',
            'Innodb_pages_written'           : 'pages_written',
            'Innodb_rows_deleted'            : 'rows_deleted',
            'Innodb_rows_inserted'           : 'rows_inserted',
            'Innodb_rows_read'               : 'rows_read',
            'Innodb_rows_updated'            : 'rows_updated',
        }
        
        # If the SHOW STATUS value exists, override...
        for k,v in overrides.items():
            if k in status.keys():
                log_debug('Override %s' % (k))
                istatus_vals[v] = status[k]
                
        # Now copy the values into $status.
        for k in istatus_vals.keys():
            status[k] = istatus_vals[k]
            
    # Make table_open_cache backwards-compatible (issue 63).
    if 'table_open_cache' in status.keys():
        status['table_cache'] = status.get('table_open_cache')
        
    # Compute how much of the key buffer is used and unflushed (issue 127).
    status['Key_buf_bytes_used'] = big_sub(status.get('key_buffer_size'), big_multiply(status.get('Key_blocks_unused'), status.get('key_cache_block_size')))
    status['Key_buf_bytes_unflushed'] = big_multiply(status.get('Key_blocks_not_flushed'), status.get('key_cache_block_size'))
    
    if 'unflushed_log' in status.keys() and status.get('unflushed_log'):
        # TODO: I'm not sure what the deal is here; need to debug this.  But the
        # unflushed log bytes spikes a lot sometimes and it's impossible for it to
        # be more than the log buffer.
        log_debug('Unflushed log: %s' % (status.get('unflushed_log')))
        status['unflushed_log'] = max(status.get('unflushed_log'), status.get('innodb_log_buffer_size'))
        
    keys = [
        'Key_read_requests',
        'Key_reads',
        'Key_write_requests',
        'Key_writes',
        'history_list',
        'innodb_transactions',
        'read_views',
        'current_transactions',
        'locked_transactions',
        'active_transactions',
        'pool_size',
        'free_pages',
        'database_pages',
        'modified_pages',
        'pages_read',
        'pages_created',
        'pages_written',
        'file_fsyncs',
        'file_reads',
        'file_writes',
        'log_writes',
        'pending_aio_log_ios',
        'pending_aio_sync_ios',
        'pending_buf_pool_flushes',
        'pending_chkp_writes',
        'pending_ibuf_aio_reads',
        'pending_log_flushes',
        'pending_log_writes',
        'pending_normal_aio_reads',
        'pending_normal_aio_writes',
        'ibuf_inserts',
        'ibuf_merged',
        'ibuf_merges',
        'spin_waits',
        'spin_rounds',
        'os_waits',
        'rows_inserted',
        'rows_updated',
        'rows_deleted',
        'rows_read',
        'Table_locks_waited',
        'Table_locks_immediate',
        'Slow_queries',
        'Open_files',
        'Open_tables',
        'Opened_tables',
        'innodb_open_files',
        'open_files_limit',
        'table_cache',
        'Aborted_clients',
        'Aborted_connects',
        'Max_used_connections',
        'Slow_launch_threads',
        'Threads_cached',
        'Threads_connected',
        'Threads_created',
        'Threads_running',
        'max_connections',
        'thread_cache_size',
        'Connections',
        'slave_running',
        'slave_stopped',
        'Slave_retried_transactions',
        'slave_lag',
        'Slave_open_temp_tables',
        'Qcache_free_blocks',
        'Qcache_free_memory',
        'Qcache_hits',
        'Qcache_inserts',
        'Qcache_lowmem_prunes',
        'Qcache_not_cached',
        'Qcache_queries_in_cache',
        'Qcache_total_blocks',
        'query_cache_size',
        'Questions',
        'Com_update',
        'Com_insert',
        'Com_select',
        'Com_delete',
        'Com_replace',
        'Com_load',
        'Com_update_multi',
        'Com_insert_select',
        'Com_delete_multi',
        'Com_replace_select',
        'Select_full_join',
        'Select_full_range_join',
        'Select_range',
        'Select_range_check',
        'Select_scan',
        'Sort_merge_passes',
        'Sort_range',
        'Sort_rows',
        'Sort_scan',
        'Created_tmp_tables',
        'Created_tmp_disk_tables',
        'Created_tmp_files',
        'Bytes_sent',
        'Bytes_received',
        'innodb_log_buffer_size',
        'unflushed_log',
        'log_bytes_flushed',
        'log_bytes_written',
        'relay_log_space',
        'binlog_cache_size',
        'Binlog_cache_disk_use',
        'Binlog_cache_use',
        'binary_log_space',
        'innodb_locked_tables',
        'innodb_lock_structs',
        'State_closing_tables',
        'State_copying_to_tmp_table',
        'State_end',
        'State_freeing_items',
        'State_init',
        'State_locked',
        'State_login',
        'State_preparing',
        'State_reading_from_net',
        'State_sending_data',
        'State_sorting_result',
        'State_statistics',
        'State_updating',
        'State_writing_to_net',
        'State_none',
        'State_other',
        'Handler_commit',
        'Handler_delete',
        'Handler_discover',
        'Handler_prepare',
        'Handler_read_first',
        'Handler_read_key',
        'Handler_read_next',
        'Handler_read_prev',
        'Handler_read_rnd',
        'Handler_read_rnd_next',
        'Handler_rollback',
        'Handler_savepoint',
        'Handler_savepoint_rollback',
        'Handler_update',
        'Handler_write',
        'innodb_tables_in_use',
        'innodb_lock_wait_secs',
        'hash_index_cells_total',
        'hash_index_cells_used',
        'total_mem_alloc',
        'additional_pool_alloc',
        'uncheckpointed_bytes',
        'ibuf_used_cells',
        'ibuf_free_cells',
        'ibuf_cell_count',
        'adaptive_hash_memory',
        'page_hash_memory',
        'dictionary_cache_memory',
        'file_system_memory',
        'lock_system_memory',
        'recovery_system_memory',
        'thread_hash_memory',
        'innodb_sem_waits',
        'innodb_sem_wait_time_ms',
        'Key_buf_bytes_unflushed',
        'Key_buf_bytes_used',
        'key_buffer_size',
        'Innodb_row_lock_time',
        'Innodb_row_lock_waits',
        'Query_time_count_00',
        'Query_time_count_01',
        'Query_time_count_02',
        'Query_time_count_03',
        'Query_time_count_04',
        'Query_time_count_05',
        'Query_time_count_06',
        'Query_time_count_07',
        'Query_time_count_08',
        'Query_time_count_09',
        'Query_time_count_10',
        'Query_time_count_11',
        'Query_time_count_12',
        'Query_time_count_13',
        'Query_time_total_00',
        'Query_time_total_01',
        'Query_time_total_02',
        'Query_time_total_03',
        'Query_time_total_04',
        'Query_time_total_05',
        'Query_time_total_06',
        'Query_time_total_07',
        'Query_time_total_08',
        'Query_time_total_09',
        'Query_time_total_10',
        'Query_time_total_11',
        'Query_time_total_12',
        'Query_time_total_13',
    ] 
    
    # Return the output.
    output = []
    for k in keys:
        # If the value isn't defined, return -1 which is lower than (most graphs')
        # minimum value of 0, so it'll be regarded as a missing value.
        val = status.get(k) if status.get(k) is not None else -1
        output.append('%s:%s' % (k, str(val)))
        
    result = ' '.join(output)
    if fp is not None:
        with open(cache_file, 'w+') as fp:
            fp.write('%s\n' % result)
    db.close()
    return result
        
# ============================================================================
# A drop-in replacement for PHP's array_change_key_case
# ============================================================================
def dict_change_key_case(input, case='lower'):
    CASE_LOWER = 'lower'
    CASE_UPPER = 'upper'
    if case == CASE_LOWER:
        f = str.lower
    elif case == CASE_UPPER:
        f = str.upper
    else:
        raise ValueError()
    return dict((f(k), v) for k, v in input.items())

# ============================================================================
# A drop-in replacement for PHP's base_convert    
# ============================================================================
def base_convert(number, fromBase, toBase):
    try:
        # Convert number to base 10
        base10 = int(number, fromBase)
    except ValueError:
        raise
        
    if toBase < 2 or toBase > 36:
        raise NotImplementedError
        
    output_value = ''
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    sign = ''
    
    if base10 == 0:
        return '0'
    elif base10 < 0:
        sign = '-'
        base10 = -base10
    
    # Convert to base toBase    
    s = ''
    while base10 != 0:
        r = base10 % toBase
        r = int(r)
        s = digits[r] + s
        base10 //= toBase
        
    output_value = sign + s
    return output_value
        
# ============================================================================
# Given INNODB STATUS text, returns a dictionary of the parsed text.  Each
# line shows a sample of the input for both standard InnoDB as you would find in
# MySQL 5.0, and XtraDB or enhanced InnoDB from Percona if applicable.  Note
# that extra leading spaces are ignored due to trim().
# ============================================================================        
def get_innodb_array(text):
    result = {
        'spin_waits'                : [],
        'spin_rounds'               : [],
        'os_waits'                  : [],
        'pending_normal_aio_reads'  : None,
        'pending_normal_aio_writes' : None,
        'pending_ibuf_aio_reads'    : None,
        'pending_aio_log_ios'       : None,
        'pending_aio_sync_ios'      : None,
        'pending_log_flushes'       : None,
        'pending_buf_pool_flushes'  : None,
        'file_reads'                : None,
        'file_writes'               : None,
        'file_fsyncs'               : None,
        'ibuf_inserts'              : None,
        'ibuf_merged'               : None,
        'ibuf_merges'               : None,
        'log_bytes_written'         : None,
        'unflushed_log'             : None,
        'log_bytes_flushed'         : None,
        'pending_log_writes'        : None,
        'pending_chkp_writes'       : None,
        'log_writes'                : None,
        'pool_size'                 : None,
        'free_pages'                : None,
        'database_pages'            : None,
        'modified_pages'            : None,
        'pages_read'                : None,
        'pages_created'             : None,
        'pages_written'             : None,
        'queries_inside'            : None,
        'queries_queud'             : None,
        'read_views'                : None,
        'rows_inserted'             : None,
        'rows_updated'              : None,
        'rows_deleted'              : None,
        'rows_read'                 : None,
        'innodb_transactions'       : None,
        'unpurged_txns'             : None,
        'history_list'              : None,
        'current_transactions'      : None,
        'hash_index_cells_total'    : None,
        'hash_index_cells_used'     : None,
        'total_mem_alloc'           : None,
        'additional_pool_alloc'     : None,
        'last_checkpoint'           : None,
        'uncheckpointed_bytes'      : None,
        'ibuf_used_cells'           : None,
        'ibuf_free_cells'           : None,
        'ibuf_cell_count'           : None,
        'adaptive_hash_memory'      : None,
        'page_hash_memory'          : None,
        'dictionary_cache_memory'   : None,
        'file_system_memory'        : None,
        'lock_system_memory'        : None,
        'recovery_system_memory'    : None,
        'thread_hash_memory'        : None,
        'innodb_sem_waits'          : None,
        'innodb_sem_wait_time_ms'   : None,
    }
    txn_seen = False
    for line in text.split('\n'):
        line =  line.strip()
        row = re.split(' +', line)
        
        # SEMAPHORES
        if line.find('Mutex spin waits') == 0:
            # Mutex spin waits 79626940, rounds 157459864, OS waits 698719
            # Mutex spin waits 0, rounds 247280272495, OS waits 316513438
            result['spin_waits'].append(to_int(row[3]))
            result['spin_rounds'].append(to_int(row[5]))
            result['os_waits'].append(to_int(row[8]))
        elif line.find('RW-shared spins') == 0 and line.find(';') > 0:
            # RW-shared spins 3859028, OS waits 2100750; RW-excl spins 4641946, OS waits 1530310
            result['spin_waits'].append(to_int(row[2]))
            result['spin_waits'].append(to_int(row[8]))
            result['os_waits'].append(to_int(row[5]))
            result['os_waits'].append(to_int(row[11]))
        elif line.find('RW-shared spins') == 0 and line.find('; RW-excl spins') == -1:
            # Post 5.5.17 SHOW ENGINE INNODB STATUS syntax
            # RW-shared spins 604733, rounds 8107431, OS waits 241268
            result['spin_waits'].append(to_int(row[2]))
            result['os_waits'].append(to_int(row[7]))
        elif line.find('RW-excl spins') == 0:
            # Post 5.5.17 SHOW ENGINE INNODB STATUS syntax
            # RW-excl spins 604733, rounds 8107431, OS waits 241268
            result['spin_waits'].append(to_int(row[2]))
            result['os_waits'].append(to_int(row[7]))
        elif line.find('seconds the semaphore:') > 0:
            # --Thread 907205 has waited at handler/ha_innodb.cc line 7156 for 1.00 seconds the semaphore:
            increment(result, 'innodb_sem_waits', 1)
            increment(result, 'innodb_sem_wait_time_ms', to_int(row[9])*1000)
        
        # TRANSACTIONS
        elif line.find('Trx id counter') == 0:
            # The beginning of the TRANSACTIONS section: start counting
            # transactions
            # Trx id counter 0 1170664159
            # Trx id counter 861B144C
            try:
                val = make_bigint(row[3], row[4])
            except IndexError:
                val = make_bigint(row[3], None)
            result['innodb_transactions'] = val
            txn_seen = True
        elif line.find('Purge done for trx') == 0:
            # Purge done for trx's n:o < 0 1170663853 undo n:o < 0 0
            # Purge done for trx's n:o < 861B135D undo n:o < 0
            purged_to = make_bigint(row[6], None if row[7] == 'undo' else row[7])
            result['unpurged_txns'] = big_sub(result.get('innodb_transactions'), purged_to)
        elif line.find('History list length') == 0:
            # History list length 132
            result['history_list'] = to_int(row[3])
        elif txn_seen and line.find('---TRANSACTION') == 0:
            # ---TRANSACTION 0, not started, process no 13510, OS thread id 1170446656
            increment(result, 'current_transactions', 1)
            if line.find('ACTIVE') > 0:
                increment(result, 'active_transactions', 1)
        elif txn_seen and line.find('------- TRX HAS BEEN') == 0:
            # ------- TRX HAS BEEN WAITING 32 SEC FOR THIS LOCK TO BE GRANTED:
            increment(result, 'innodb_lock_wait_secs', to_int(row[5]))
        elif line.find('read views open inside InnoDB') > 0:
            # 1 read views open inside InnoDB
            result['read_views'] = to_int(row[0])
        elif line.find('mysql tables in use') == 0:
             # mysql tables in use 2, locked 2
             increment(result, 'innodb_tables_in_use', to_int(row[4]))
             increment(result, 'innodb_locked_tables', to_int(row[6]))
        elif txn_seen and line.find('lock struct(s)') > 0:
            # 23 lock struct(s), heap size 3024, undo log entries 27
            # LOCK WAIT 12 lock struct(s), heap size 3024, undo log entries 5
            # LOCK WAIT 2 lock struct(s), heap size 368
            if line.find('LOCK WAIT') == 0:
                increment(result, 'innodb_lock_structs', to_int(row[2]))
                increment(result, 'locked_transactions', 1)
            else:
                increment(result, 'innodb_lock_structs', to_int(row[0]))
        
        # FILE I/O
        elif line.find(' OS file reads, ') > 0:
            # 8782182 OS file reads, 15635445 OS file writes, 947800 OS fsyncs
            result['file_reads'] = to_int(row[0])
            result['file_writes'] = to_int(row[4])
            result['file_fsyncs'] = to_int(row[8])
        elif line.find('Pending normal aio reads:') == 0:
            # Pending normal aio reads: 0, aio writes: 0,
            result['pending_normal_aio_reads'] = to_int(row[4])
            result['pending_normal_aio_writes'] = to_int(row[7])
        elif line.find('ibuf aio reads') == 0:
            # ibuf aio reads: 0, log i/o's: 0, sync i/o's: 0
            result['pending_ibuf_aio_reads'] = to_int(row[3])
            result['pending_aio_log_ios'] = to_int(row[6])
            result['pending_aio_sync_ios'] = to_int(row[9])
        elif line.find('Pending flushes (fsync)') == 0:
            # Pending flushes (fsync) log: 0; buffer pool: 0
            result['pending_log_flushes'] = to_int(row[4])
            result['pending_buf_pool_flushes'] = to_int(row[7])
            
        # INSERT BUFFER AND ADAPTIVE HASH INDEX
        elif line.find('Ibuf for space 0: size ') == 0:
            # Older InnoDB code seemed to be ready for an ibuf per tablespace.  It
            # had two lines in the output.  Newer has just one line, see below.
            # Ibuf for space 0: size 1, free list len 887, seg size 889, is not empty
            # Ibuf for space 0: size 1, free list len 887, seg size 889,
            result['ibuf_used_cells'] = to_int(row[5])
            result['ibuf_free_cells'] = to_int(row[9])
            result['ibuf_cell_count'] = to_int(row[12])
        elif line.find('Ibuf: size ') == 0:
            # Ibuf: size 1, free list len 4634, seg size 4636,
            result['ibuf_used_cells'] = to_int(row[2])
            result['ibuf_free_cells'] = to_int(row[6])
            result['ibuf_cell_count'] = to_int(row[9])
            if line.find('merges'):
                result['ibuf_merges'] = to_int(row[10])
        elif line.find('delete mark ') > 0 and prev_line.find('merged operations:') == 0:
            # Output of show engine innodb status has changed in 5.5
            # merged operations:
            # insert 593983, delete mark 387006, delete 73092
            result['ibuf_inserts'] = to_int(row[1])
            result['ibuf_merged'] = to_int(row[1]) + to_int(row[4]) + to_int(row[6])
        elif line.find('merged recs, ') > 0:
            # 19817685 inserts, 19817684 merged recs, 3552620 merges
            result['ibuf_inserts'] = to_int(row[0])
            result['ibuf_merged'] = to_int(row[2])
            result['ibuf_merges'] = to_int(row[5])
        elif line.find('Hash table size ') == 0:
            # In some versions of InnoDB, the used cells is omitted.
            # Hash table size 4425293, used cells 4229064, ....
            # Hash table size 57374437, node heap has 72964 buffer(s) <-- no used cells
            result['hash_index_cells_total'] = to_int(row[3])
            result['hash_index_cells_used'] = to_int(row[6]) if line.find('used cells') > 0 else '0'
            
        # LOG
        elif line.find(' log i/o\'s done, ') > 0:
            # 3430041 log i/o's done, 17.44 log i/o's/second
            # 520835887 log i/o's done, 17.28 log i/o's/second, 518724686 syncs, 2980893 checkpoints
            # TODO: graph syncs and checkpoints
            result['log_writes'] = to_int(row[0])
        elif line.find(' pending log writes, ') > 0:
            # 0 pending log writes, 0 pending chkp writes
            result['pending_log_writes'] = to_int(row[0])
            result['pending_chkp_writes'] = to_int(row[4])
        elif line.find('Log sequence number') == 0:
            # This number is NOT printed in hex in InnoDB plugin.
            # Log sequence number 13093949495856 //plugin
            # Log sequence number 125 3934414864 //normal
            try:
                val = make_bigint(row[3], row[4])
            except IndexError:
                val = to_int(row[3])
            result['log_bytes_written'] = val
        elif line.find('Log flushed up to') == 0:
            # This number is NOT printed in hex in InnoDB plugin.
            # Log flushed up to   13093948219327
            # Log flushed up to   125 3934414864
            try:
                val = make_bigint(row[4], row[5])
            except IndexError:
                val = to_int(row[4])
            result['log_bytes_flushed'] = val
        elif line.find('Last checkpoint at') == 0:
            # Last checkpoint at  125 3934293461
            try:
                val = make_bigint(row[3], row[4])
            except IndexError:
                val = to_int(row[3])
            result['last_checkpoint'] = val
            
        # BUFFER POOL AND MEMORY
        elif line.find('Total memory allocated') == 0:
            # Total memory allocated 29642194944; in additional pool allocated 0
            result['total_mem_alloc'] = to_int(row[3])
            result['additional_pool_alloc'] = to_int(row[8])
        elif line.find('Adaptive hash index ') == 0:
            #   Adaptive hash index 1538240664 	(186998824 + 1351241840)
            result['adaptive_hash_memory'] = to_int(row[3])
        elif line.find('Page hash           ') == 0:
            #   Page hash           11688584
            result['page_hash_memory'] = to_int(row[2])
        elif line.find('Dictionary cache    ') == 0:
            #   Dictionary cache    145525560 	(140250984 + 5274576)
            result['dictionary_cache_memory'] = to_int(row[2])
        elif line.find('File system         ') == 0:
            #   File system         313848 	(82672 + 231176)
            result['file_system_memory'] = to_int(row[2])
        elif line.find('Lock system         ') == 0:
            #   Lock system         29232616 	(29219368 + 13248)
            result['lock_system_memory'] = to_int(row[2])
        elif line.find('Recovery system     ') == 0:
            #   Recovery system     0 	(0 + 0)
            result['recovery_system_memory'] = to_int(row[2])
        elif line.find('Threads             ') == 0:
            #   Threads             409336 	(406936 + 2400)
            result['thread_hash_memory'] = to_int(row[1])
        elif line.find('Buffer pool size ') == 0:
            # The " " after size is necessary to avoid matching the wrong line:
            # Buffer pool size        1769471
            # Buffer pool size, bytes 28991012864
            result['pool_size'] = to_int(row[3])
        elif line.find('Free buffers') == 0:
            # Free buffers            0
            result['free_pages'] = to_int(row[2])
        elif line.find('Database pages') == 0:
            # Database pages          1696503
            result['database_pages'] = to_int(row[2])
        elif line.find('Modified db pages') == 0:
            # Modified db pages       160602
            result['modified_pages'] = to_int(row[3])
        elif line.find('Pages read ahead') == 0:
             # Must do this BEFORE the next test, otherwise it'll get fooled by this
            # line from the new plugin (see samples/innodb-015.txt):
            # Pages read ahead 0.00/s, evicted without access 0.06/s
            # TODO: No-op for now, see issue 134.
            pass
        elif line.find('Pages read') == 0:
            # Pages read 15240822, created 1770238, written 21705836
            result['pages_read'] = to_int(row[2])
            result['pages_created'] = to_int(row[4])
            result['pages_written'] = to_int(row[6])
            
        # ROW OPERATIONS
        elif line.find('Number of rows inserted') == 0:
            # Number of rows inserted 50678311, updated 66425915, deleted 20605903, read 454561562
            result['rows_inserted'] = to_int(row[4])
            result['rows_updated'] = to_int(row[6])
            result['rows_deleted'] = to_int(row[8])
            result['rows_read'] = to_int(row[10])
        elif line.find(' queries inside InnoDB, ') > 0:
            # 0 queries inside InnoDB, 0 queries in queue
            result['queries_inside'] = to_int(row[0])
            result['queries_queued'] = to_int(row[4])
        
        prev_line = line
    for key in ['spin_waits', 'spin_rounds', 'os_waits']:
        result[key] = sum(result.get(key))
    result['unflushed_log'] = big_sub(result.get('log_bytes_written'), result.get('log_bytes_flushed'))
    result['uncheckpointed_bytes'] = big_sub(result.get('log_bytes_written'), result.get('last_checkpoint'))
    
    return result

# ============================================================================
# Returns a bigint from two ulint or a single hex number.
# ============================================================================
def make_bigint(hi, lo=None):
    log_debug([hi, lo])
    if lo is None:
        return base_convert(hi, 16, 10)
    else:
        hi = hi if hi else '0'
        lo = lo if lo else '0'
        return big_add(big_multiply(hi, 4294967296), lo)
        
# ============================================================================
# Extracts the numbers from a string.
# ============================================================================
def to_int(val):
    log_debug(val)
    m = re.search('\d+', val)
    if m:
        val = long(m.group())
    else:
        val = 0
    return val

# ============================================================================
# Safely increments a value that might be null.
# ============================================================================
def increment(dictionary, key, how_much):
    log_debug([key, how_much])
    if key in dictionary.keys() and dictionary.get(key) is not None:
        dictionary[key] = big_add(dictionary.get(key), how_much)
    else:
        dictionary[key] = how_much
 
# ============================================================================
# Multiply two big integers together
# ============================================================================
def big_multiply(left, right):
    if not isinstance(left, (int, long, float, complex)):
        try:
            left = long(left)
        except Exception:
            left = 0
    if not isinstance(right, (int, long, float, complex)):
        try:
            right = long(right)
        except Exception:
            right = 0
    return left * right

# ============================================================================
# Subtract two big integers
# ============================================================================
def big_sub(left, right):
    if not isinstance(left, (int, long, float, complex)):
        try:
            left = long(left)
        except Exception:
            left = 0
    if not isinstance(right, (int, long, float, complex)):
        try:
            right = long(right)
        except Exception:
            right = 0
    return left - right
    
# ============================================================================
# Add two big integers together
# ============================================================================
def big_add(left, right):
    if not isinstance(left, (int, long, float, complex)):
        try:
            left = long(left)
        except Exception:
            left = 0
    if not isinstance(right, (int, long, float, complex)):
        try:
            right = long(right)
        except Exception:
            right = 0
    return left + right
    
# ============================================================================
# Writes to a debugging log.
# ============================================================================
def log_debug(val):
    global debug, debug_log
    if not debug and not debug_log:
        return
    try:
        with open(debug_log, 'a+') as f:
            trace = traceback.extract_stack()
            calls = []
            i = 0
            line = 0
            file_name = ''
            for arr in reversed(trace):
                calls.append('%s at %s:%d' % (arr[2], arr[0], arr[1]))
                file_name = arr[0]
                line = arr[1]
            calls.pop(0)
            if len(calls) == 0:
                calls.append('at %s:%d' % (file_name, line))
            now = datetime.now()
            str_now = now.strftime('%Y-%m-%d %H:%M:%S')
            f.write('%s %s\n' % (str_now, ' <- '.join(calls)))
            f.write('%s\n' % (val.__repr__()))
                    
    except Exception, e:
        print e
        print 'Warning: disabling debug logging to %s\n' % (debug_log)
        debug_log = False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--host', help='Hostname to connect to', required=True)
    parser.add_argument('--port', help='Database Port', default=mysql_port, type=int)
    parser.add_argument('--user', help='MySQL username', default=mysql_user)
    parser.add_argument('--password', help='MySQL password', default=mysql_pass)
    parser.add_argument('--heartbeat', help='MySQL heartbeat table (see pt-heartbeat)', default=heartbeat)
    parser.add_argument('--nocache', help='Do not cache results in a file', action='store_true')
    parser.add_argument('--graphite-host', help='Graphite host', default=graphite_host)
    parser.add_argument('--graphite-port', help='Graphite port', default=graphite_port, type=int)
    parser.add_argument('--use-graphite', help='Send stats to Graphite', action='store_true')
    
    args = parser.parse_args()
    log_debug(args)
    
    result = ss_get_mysql_stats(args)
    log_debug(result)
    
    output = []
    unix_ts = int(time.time())
    sanitized_host = args.host.replace(':', '').replace('.', '').replace('/', '_')
    sanitized_host = sanitized_host + '_' + str(args.port)
    for stat in result.split():
        var_name, val = stat.split(':')
        output.append('mysql.%s.%s %s %d' % (sanitized_host, var_name, val, unix_ts))   
    output = set(output)
    log_debug(['Final result', output])
    if args.use_graphite:
        # Send to graphite
        sock = socket()
        try:
            sock.connect((args.graphite_host, args.graphite_port))
        except Exception:
            print "Couldn't connect to %s on port %d, is carbon-agent.py running?" % (args.graphite_host, args.graphite_port)
            sys.exit(1)
            
        message = '\n'.join(output) + '\n'
        sock.sendall(message)
        sock.close()
    else:
        print '\n'.join(output)
    
