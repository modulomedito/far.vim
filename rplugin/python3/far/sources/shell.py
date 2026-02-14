"""
File: shell.py
Description: shell command source
Author: Oleg Khalidov <brooth@gmail.com>
License: MIT
"""

from pprint import pprint
from locale import getpreferredencoding
from sys import platform

from .far_glob import load_ignore_rules, far_glob, GlobError, IgnoreFileError, rg_rules_glob, rg_ignore_globs
import logging
import subprocess
import re
import os
import tempfile
import pathlib
import json
import io
from json import JSONDecodeError

logger = logging.getLogger('far')


class MultiProc:
    def __init__(self, cmd, files, cwd):
        self.cmd = cmd
        self.files = files
        self.cwd = cwd
        self.file_idx = 0
        self.proc = None
        self.returncode = None
        self.ARG_MAX = 30000
        self.all_stderr = b''

        self._next_proc()

    def _next_proc(self):
        if self.proc:
            try:
                err = self.proc.stderr.read()
                if err:
                    self.all_stderr += err
            except Exception as e:
                logger.debug('MultiProc read stderr error: ' + str(e))

            self.proc.wait()

        if self.file_idx >= len(self.files):
            self.proc = None
            self.returncode = 0
            return

        current_cmd = list(self.cmd)
        cmd_len = sum(len(arg) + 1 for arg in current_cmd)

        chunk = []
        while self.file_idx < len(self.files):
            f = self.files[self.file_idx]
            arg_len = len(f) + 3
            if cmd_len + arg_len > self.ARG_MAX:
                if not chunk:
                    chunk.append(f)
                    self.file_idx += 1
                break

            chunk.append(f)
            cmd_len += arg_len
            self.file_idx += 1

        current_cmd.extend(chunk)
        logger.debug('MultiProc cmd: %s', str(current_cmd))

        self.proc = subprocess.Popen(current_cmd, cwd=self.cwd,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def poll(self):
        if self.proc:
            if self.proc.poll() is None:
                return None

        if self.file_idx >= len(self.files) and self.proc is None:
            return self.returncode
        return None

    @property
    def stdout(self):
        return self

    @property
    def stderr(self):
        return io.BytesIO(self.all_stderr)

    def readline(self):
        if not self.proc:
            return b''

        line = self.proc.stdout.readline()
        if not line:
            self._next_proc()
            return self.readline()

        return line

    def terminate(self):
        if self.proc:
            self.proc.terminate()


def search(ctx, args, cmdargs):
    logger.debug('search(%s, %s, %s)', str(ctx), str(args), str(cmdargs))

    final_result = {'warning': ''}

    if not args.get('cmd'):
        return {'error': 'no cmd in args'}

    source = ctx['source']
    pattern = ctx['pattern']

    # Manual parsing to handle \C and \c flags
    if source in ('rg', 'rgnvim'):
        new_pattern = []
        i = 0
        n = len(pattern)
        while i < n:
            if pattern[i] == '\\':
                # Count consecutive backslashes
                start = i
                i += 1
                while i < n and pattern[i] == '\\':
                    i += 1
                backslashes = i - start

                # Check next char
                if i < n and pattern[i] in ('C', 'c'):
                    char = pattern[i]
                    if backslashes % 2 == 1:
                        # Odd backslashes + C/c -> Flag
                        # Append (backslashes - 1) backslashes
                        new_pattern.append('\\' * (backslashes - 1))
                        if char == 'C':
                            if '--case-sensitive' not in cmdargs:
                                cmdargs.append('--case-sensitive')
                        elif char == 'c':
                            if '--ignore-case' not in cmdargs:
                                cmdargs.append('--ignore-case')
                        i += 1  # Skip C/c
                        continue
                    else:
                        # Even backslashes + C/c -> Literal
                        new_pattern.append('\\' * backslashes)
                        new_pattern.append(char)
                        i += 1
                        continue
                else:
                    # Just backslashes followed by something else
                    new_pattern.append('\\' * backslashes)
                    continue
            else:
                new_pattern.append(pattern[i])
                i += 1
        pattern = ''.join(new_pattern)

    regex = ctx['regex']
    case_sensitive = ctx['case_sensitive']

    # Ensure case_sensitive context is respected for rg
    if source in ('rg', 'rgnvim'):
        if case_sensitive == 1:
            if '--case-sensitive' not in cmdargs:
                cmdargs.append('--case-sensitive')
            # Remove conflicting flags if present
            if '--ignore-case' in cmdargs:
                cmdargs.remove('--ignore-case')
            if '--smart-case' in cmdargs:
                cmdargs.remove('--smart-case')
        elif case_sensitive == 0:
            if '--ignore-case' not in cmdargs:
                cmdargs.append('--ignore-case')
            # Remove conflicting flags if present
            if '--case-sensitive' in cmdargs:
                cmdargs.remove('--case-sensitive')
            if '--smart-case' in cmdargs:
                cmdargs.remove('--smart-case')

    file_mask = ctx['file_mask']
    submatch_type = args.get('submatch')
    root = ctx['cwd']

    limit = int(ctx['limit'])
    max_columns = args.get('max_columns')
    ignore_files = args.get('ignore_files')
    glob_mode = args.get('glob_mode', 'far')

    rules = file_mask.split(',')
    native_glob_args = None

    is_win32 = (platform == 'win32')
    preferred_encoding = getpreferredencoding()

    # Perform file globbing if non-native
    if glob_mode == 'far':
        # Use built-in globbing strategy
        ignore_rules = []
        for ignore_file in ignore_files:
            try:
                ignore_rules.extend(
                    load_ignore_rules(ignore_file)
                )
            except IgnoreFileError as e:
                final_result['warning'] += ' | Invalid ignore-rule files. ' + \
                    str(e)

        try:
            files = far_glob(root, rules, ignore_rules)
        except GlobError as e:
            return {'error': 'Invalid glob expression. '+str(e)}

        if len(files) == 0:
            return {'error': 'No files matching the glob expression'}
    elif glob_mode == 'rg':
        # Use ripgrep to glob
        rg_glob_cmd = f'rg --files --no-ignore {rg_rules_glob(rules)} {rg_ignore_globs(ignore_files)}'
        logger.debug(f'Globbing with ripgrep: {rg_glob_cmd}')
        try:
            output = subprocess.check_output(rg_glob_cmd, shell=True, cwd=root)
            files = output.decode(preferred_encoding).splitlines()
        except subprocess.CalledProcessError as e:
            logger.debug(f'rg globbing failed: {e}')
            files = []
        except Exception as e:
            return {'error': f'Globbing error: {str(e)}'}

        if len(files) == 0:
            return {'error': 'No files matching the glob expression'}
    elif glob_mode == 'native':
        # Pass the mask directly to the search tool.
        # For rg, the file mask is converted into -g option glob rules.  For everything else,
        # the mask is passed directly as an agument (and typically treated as a directory).
        if source in ('rg', 'rgnvim'):
            native_glob_args = rg_rules_glob(
                rules, False) + rg_ignore_globs(ignore_files, False)
    else:
        return {'error': 'Invalid glob_mode'}

    # Build search command
    cmd = []
    use_xargs = glob_mode != 'native' and not is_win32
    if use_xargs:
        # Run each for each globbed file
        cmd.append('xargs')
        cmd.append('-0')
    for c in args['cmd']:
        if c != '{file_mask}' or (glob_mode == 'native' and file_mask and not native_glob_args):
            cmd.append(
                c.format(limit=limit, pattern=pattern, file_mask=file_mask))
    if args.get('expand_cmdargs', '0') != '0':
        cmd += cmdargs
    if native_glob_args:
        cmd += native_glob_args

    logger.debug('cmd:' + str(cmd))
    logger.debug('pattern:' + str(pattern))
    logger.debug('cmdargs:' + str(cmdargs))

    # Determine how to handle stdin for the command
    if use_xargs:
        proc_stdin = subprocess.PIPE
    else:
        proc_stdin = subprocess.DEVNULL

    # Execute search command
    try:
        if glob_mode != 'native' and is_win32:
            proc = MultiProc(cmd, files, ctx['cwd'])
        else:
            proc = subprocess.Popen(cmd, cwd=ctx['cwd'], stdin=proc_stdin,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        return {'error': str(e)}

    # If xargs, pipe the file list to stdin
    if use_xargs:
        sep = '\0'
        proc.stdin.write((sep.join(files) + sep).encode(preferred_encoding))
        proc.stdin.close()

    logger.debug('type(proc) = ' + str(type(proc)))

    range_ = tuple(ctx['range'])
    result = {}

    if source == 'rg' or source == 'rgnvim':

        while limit > 0:
            line = proc.stdout.readline()

            try:
                line = line.decode('utf-8').rstrip()
            except UnicodeDecodeError:
                logger.debug(
                    "UnicodeDecodeError: line = line.decode('utf-8').rstrip() failed, line:")
                continue

            if not line:
                if len(result) == 0:
                    err = proc.stderr.readline()
                    if err:
                        err = err.decode('utf-8')
                        logger.debug('error:' + err)
                        return {'error': err}

                if proc.poll() is not None:
                    logger.debug('end of proc. break')
                    break
                continue

            logger.debug('proc readline: ' + line)
            try:
                item = json.loads(line)
            except JSONDecodeError as err:
                logger.debug('json error: ' + err)
                continue

            if type(item) != dict or 'type' not in item:
                logger.debug(
                    'json error: item is not dict or item has no key "type". item =' + str(item))
                continue

            if item['type'] == 'match':
                data = item['data']
                file_name = data['path']['text']
                lnum = data['line_number']

                try:
                    text = data['lines']['text']
                except KeyError:
                    text = data['lines']['bytes']
                except:
                    logger.debug(
                        "item['data']['lines'] has neigher key 'test' nor key 'bytes'. item =" + str(item))
                    continue
                if len(text) > max_columns:
                    logger.debug(
                        "File '{file_name}' line {lnum} is too long, longer than max_column {max_columns}."
                        .format(file_name=file_name, lnum=lnum, max_columns=max_columns))
                    continue
                text = text.split('\n')[0]
                text = text.rstrip()

                for submatch in data['submatches']:
                    match = submatch['match']['text']
                    cnum = submatch['start'] + 1

                    item_idx = (file_name, lnum, cnum)

                    if 'one_file_result' in locals() or 'one_file_result' in globals():
                        if item_idx in one_file_result:
                            continue
                        else:
                            one_file_result.append(item_idx)

                    if (range_[0] != -1 and range_[0] > lnum) or \
                       (range_[1] != -1 and range_[1] < lnum):
                        continue

                    if not file_name in result:
                        result[file_name] = {
                            'fname': file_name,
                            'items': []
                        }

                    item_ctx = {
                        'lnum': lnum,
                        'cnum': cnum,
                        'text': text,
                        'match': match
                    }
                    result[file_name]['items'].append(item_ctx)

                    limit -= 1

    else:
        if submatch_type == 'first':
            if regex != '0':
                try:
                    if case_sensitive == '0':
                        cpat = re.compile(pattern, re.IGNORECASE)
                    else:
                        cpat = re.compile(pattern)
                except Exception as e:
                    return {'error': 'invalid pattern: ' + str(e)}

        while limit > 0:
            line = proc.stdout.readline()

            try:
                line = line.decode('utf-8').rstrip()
            except UnicodeDecodeError:
                logger.debug(
                    "UnicodeDecodeError: line = line.decode('utf-8').rstrip() failed, line:")
                continue

            if not line:
                if len(result) == 0:
                    err = proc.stderr.readline()
                    if err:
                        err = err.decode('utf-8')
                        logger.debug('error:' + err)
                        return {'error': err}

                if proc.poll() is not None:
                    logger.debug('end of proc. break')
                    break
                continue

            items = re.split(':', line, 3)
            if len(items) != 4:
                logger.error('broken line:' + line)
                continue

            file_name = items[0]
            lnum = int(items[1])
            cnum = int(items[2])
            text = items[3]

            if (range_[0] != -1 and range_[0] > lnum) or \
               (range_[1] != -1 and range_[1] < lnum):
                continue

            if len(text) > max_columns:
                logger.debug(
                    "File '{file_name}' line {lnum} is too long, longer than max_column {max_columns}."
                    .format(file_name=file_name, lnum=lnum, max_columns=max_columns))
                continue

            item_idx = (file_name, lnum, cnum)
            if 'one_file_result' in locals() or 'one_file_result' in globals():
                if item_idx in one_file_result:
                    continue
                else:
                    one_file_result.append(item_idx)

            if not file_name in result:
                file_ctx = {
                    'fname': file_name,
                    'items': []
                }
                result[file_name] = file_ctx
            file_ctx = result[file_name]

            item_ctx = {}
            item_ctx['text'] = text
            item_ctx['lnum'] = lnum
            item_ctx['cnum'] = cnum
            file_ctx['items'].append(item_ctx)
            limit -= 1

            if submatch_type == 'first':
                byte_num = item_ctx['cnum']
                char_num = len(text.encode('utf-8')
                               [:byte_num-1].decode('utf-8'))
                move_cnum = char_num + 1

                if regex == '0':
                    while True:
                        next_item_ctx = {}
                        next_item_ctx['text'] = text
                        next_item_ctx['lnum'] = int(lnum)
                        if case_sensitive == '0':
                            next_char_num = text.lower().find(pattern.lower(), move_cnum)
                        else:
                            next_char_num = text.find(pattern, move_cnum)
                        if next_char_num == -1:
                            break
                        move_cnum = next_char_num + 1
                        prefix = text[:next_char_num]
                        next_item_ctx['cnum'] = len(prefix.encode('utf-8')) + 1
                        file_ctx['items'].append(next_item_ctx)
                        limit -= 1
                        if limit <= 0:
                            break
                else:
                    for cp in cpat.finditer(text, move_cnum):
                        next_item_ctx = {}
                        next_item_ctx['text'] = text
                        next_item_ctx['lnum'] = int(lnum)
                        prefix = text[:cp.span()[0]]
                        next_item_ctx['cnum'] = len(prefix.encode('utf-8')) + 1
                        file_ctx['items'].append(next_item_ctx)
                        limit -= 1
                        if limit <= 0:
                            break

    try:
        proc.terminate()
    except Exception as e:
        logger.error('failed to terminate proc: ' + str(e))

    with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as fp:
        for file_ctx in result.values():
            json.dump(file_ctx, fp, ensure_ascii=False)
            fp.write('\n')

    logger.debug('items_file:' + fp.name)
    final_result['items_file'] = fp.name

    return final_result
