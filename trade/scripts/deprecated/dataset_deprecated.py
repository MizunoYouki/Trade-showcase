import os
import sqlite3
import sys
from json import dumps, loads

import numpy as np
import pandas as pd

from chart.writer import revert_filename, safe_filename
from trade import config
from trade.execution.model import Execution
from trade.model import OHLCBar


# TODO: レビューと、printではない出力
def ohlc_to_virtual_executions(
        src, dest, ohlc_timewindow, high_offset='1second', low_offset='2second', close_offset='1second'
):
    """
closetime	open	high	low	close	volume	unknown
1562493540	1221933	1222500	1221933	1222500	1.67031	2041496.04915136
->
{"channel": "lightning_executions_FX_BTC_JPY", "message": [{"_id": 0, "exec_date": "2019-07-07T09:58:00", "side": null, "price": 1221933, "buy_child_order_acceptance_id": null, "sell_child_order_acceptance_id": null}]}
{"channel": "lightning_executions_FX_BTC_JPY", "message": [{"_id": 1, "exec_date": "2019-07-07T09:58:01", "side": null, "price": 1222500, "buy_child_order_acceptance_id": null, "sell_child_order_acceptance_id": null}]}
{"channel": "lightning_executions_FX_BTC_JPY", "message": [{"_id": 2, "exec_date": "2019-07-07T09:58:02", "side": null, "price": 1221933, "buy_child_order_acceptance_id": null, "sell_child_order_acceptance_id": null}]}
{"channel": "lightning_executions_FX_BTC_JPY", "message": [{"_id": 3, "exec_date": "2019-07-07T09:58:59", "side": null, "price": 1222500, "buy_child_order_acceptance_id": null, "sell_child_order_acceptance_id": null}]}
    """

    def _to_json(execution):
        return dumps({"channel": "lightning_executions_FX_BTC_JPY", "message": [execution]})

    ohlc_timewindow = pd.to_timedelta(ohlc_timewindow, 'ns')
    print('OHLC time window: {}'.format(ohlc_timewindow))

    high_offset = pd.to_timedelta(high_offset, 'ns')
    low_offset = pd.to_timedelta(low_offset, 'ns')
    close_offset = pd.to_timedelta(close_offset, 'ns')
    print('high offset: {}'.format(high_offset))
    print('low offset: {}'.format(low_offset))
    print('close offset: {}'.format(close_offset))

    _id = 0
    prev = None

    with open(src) as rfd:
        with open(dest, 'w') as wfd:
            for line in rfd:
                line = line.strip()
                if line.startswith('closetime'):
                    continue

                e = line.split()
                e.pop(-1)
                e.pop(-1)
                closetime, _open, high, low, _close = [int(v) for v in e]

                open_timestamp = np.datetime64(closetime - ohlc_timewindow.seconds, 's', utc=True)

                first = {
                    'id': _id, 'exec_date': open_timestamp.item().isoformat(),
                    'side': None, 'size': 0.01, 'price': _open,
                    'buy_child_order_acceptance_id': 'dummy', 'sell_child_order_acceptance_id': 'dummy',
                }
                wfd.write(_to_json(first))
                wfd.write('\n')
                _id += 1

                # Previous bar is positive
                if not prev or 0 < prev.close - prev.open:
                    second = {
                        'id': _id, 'exec_date': (open_timestamp + high_offset).isoformat(),
                        'side': None, 'size': 0.01, 'price': high,
                        'buy_child_order_acceptance_id': 'dummy', 'sell_child_order_acceptance_id': 'dummy',
                    }
                    wfd.write(_to_json(second))
                    wfd.write('\n')
                    _id += 1

                    third = {
                        'id': _id, 'exec_date': (open_timestamp + low_offset).isoformat(),
                        'side': None, 'size': 0.01, 'price': low,
                        'buy_child_order_acceptance_id': 'dummy', 'sell_child_order_acceptance_id': 'dummy',
                    }
                    wfd.write(_to_json(third))
                    wfd.write('\n')
                    _id += 1

                # Previous bar is negative
                else:
                    # delta(center to extreme) low > delta high, Bear trend ?
                    second = {
                        'id': _id, 'exec_date': (open_timestamp + high_offset).isoformat(),
                        'side': None, 'size': 0.01, 'price': low,
                        'buy_child_order_acceptance_id': 'dummy', 'sell_child_order_acceptance_id': 'dummy',
                    }
                    wfd.write(_to_json(second))
                    wfd.write('\n')
                    _id += 1

                    third = {
                        'id': _id, 'exec_date': (open_timestamp + low_offset).isoformat(),
                        'side': None, 'size': 0.01, 'price': high,
                        'buy_child_order_acceptance_id': 'dummy', 'sell_child_order_acceptance_id': 'dummy',
                    }
                    wfd.write(_to_json(third))
                    wfd.write('\n')
                    _id += 1

                fourth = {
                    'id': _id,
                    'exec_date': (open_timestamp + close_offset).isoformat(),
                    'side': None, 'size': 0.01, 'price': _close,
                    'buy_child_order_acceptance_id': 'dummy', 'sell_child_order_acceptance_id': 'dummy',
                }
                wfd.write(_to_json(fourth))
                wfd.write('\n')
                _id += 1

                prev = OHLCBar(ohlc_timewindow).from_variable(_open=_open, high=high, low=low, _close=_close,
                                                              open_at=open_timestamp)


def parse_local_sqlite3_filename(filename):
    filename = filename.rsplit('.', maxsplit=1)[0]
    e = filename.split('_')
    if len(e) == 4:
        exchange, symbol, first, last = e
        origin = 'native'
    elif len(e) == 5:
        exchange, symbol, first, last, origin = e
        origin = origin.lstrip('_')
    else:
        raise Exception('Unexpected length: {} ({})'.format(len(e), e))

    first_id, first_timestamp = first.split('-', maxsplit=1)
    first_id = int(first_id)
    first_timestamp = np.datetime64(revert_filename(first_timestamp))
    last_id, last_timestamp = last.split('-', maxsplit=1)
    last_id = int(last_id)
    last_timestamp = np.datetime64(revert_filename(last_timestamp))

    return dict(
        exchange=exchange, symbol=symbol, first_id=first_id, first_timestamp=first_timestamp,
        last_id=last_id, last_timestamp=last_timestamp, origin=origin
    )


class Updater:

    def __init__(self, logger, symbol_dir, symbol, exchange, timeunit='1minute', append_after=None):
        self._logger = logger

        if symbol not in ('FXBTCJPY', 'BTCJPY'):
            raise Exception('Unexpected symbol: {}'.format(symbol))
        self._symbol = symbol

        self._symbol_dir = symbol_dir
        self._exchange = exchange
        self._timeunit = pd.to_timedelta(timeunit)

        if append_after:
            self._append_after = np.datetime64(append_after, 'ns', utc=True)
        else:
            self._append_after = None

        self._logger.info('append equal or after: {}'.format(self._append_after))

    def log_to_sqlite3(self, pre_process_name, process_name):
        task = self.log_to_sqlite3.__name__

        source = os.path.join(self._symbol_dir, pre_process_name)
        destination = os.path.join(self._symbol_dir, process_name)

        if self._symbol == 'FXBTCJPY':
            channel = 'lightning_executions_FX_BTC_JPY'
        elif self._symbol == 'BTCJPY':
            channel = 'lightning_executions_BTC_JPY'

        if not os.path.exists(destination):
            os.makedirs(destination)

        source_filenames = os.listdir(source)
        source_filenames.sort()

        if self._append_after:
            for n, f in enumerate(source_filenames):
                source_timestamp = self._parse_s3_objectname(f)['timestamp']
                if self._append_after <= source_timestamp:
                    break
            source_index = n - 1
            if source_index < 0:
                source_index = 0
        else:
            source_index = 0

        for filename in source_filenames[source_index:]:
            dest_path = os.path.join(destination, 'temp.sqlite3')
            if os.path.exists(dest_path):
                os.remove(dest_path)

            conn = sqlite3.Connection(dest_path)
            conn.execute('CREATE TABLE executions ('
                         'id int primary key , side text, price real, size real, exec_date timestamp,'
                         'buy_child_order_acceptance_id text, sell_child_order_acceptance_id text)')
            executions = list()
            log_filepath = os.path.join(source, filename)
            with open(log_filepath) as fd:
                for line in fd:
                    if channel not in line:
                        continue

                    for dictobj in loads(line)['message']:
                        if dictobj['id'] in ('0', 0):
                            self._logger.warn('{}: ignore id==0, {}'.format(task, dictobj))
                            continue

                        executions.append(self._build_execution(
                            (
                                dictobj['id'], dictobj['side'], dictobj['price'], dictobj['size'], dictobj['exec_date'],
                                dictobj['buy_child_order_acceptance_id'], dictobj['sell_child_order_acceptance_id']
                            )
                        ))
            # try:
            #     conn.executemany('INSERT INTO executions VALUES (?, ?, ?, ?, ?, ?, ?)', executions)
            # except sqlite3.IntegrityError as err:
            #     self._logger.warn('{}: ignore {}'.format(task, err))
            conn.executemany(
                """'-- noinspection SqlInsertValues
INSERT OR IGNORE INTO executions VALUES (?, ?, ?, ?, ?, ?, ?)'""",
                [self._to_params(e) for e in self._drop_zero(executions)]
            )
            conn.commit()

            first = self._get_first(conn)
            last = self._get_last(conn)
            conn.close()

            renamed_path = os.path.join(
                destination,
                self._unparse_file_name(
                    self._exchange, self._symbol,
                    first['id'], first['timestamp'], last['id'], last['timestamp'], 'sqlite3'
                )
            )
            if os.path.exists(renamed_path):
                os.unlink(renamed_path)
            os.rename(dest_path, renamed_path)
            self._logger.info('{}: {}'.format(task, renamed_path))

    def detect_overlapped_sqlite3(self, process_name):
        """
        deprecated: 隣接する2ファイルでファイル名を比較し、timestampに重なりがある場合警告する
        """
        task = self.detect_overlapped_sqlite3.__name__

        filenames = os.listdir(os.path.join(self._symbol_dir, process_name))
        filenames.sort(key=lambda f: parse_local_sqlite3_filename(f)['first_id'])
        for f1, f2 in pairwise(filenames):
            last_timestamp = parse_local_sqlite3_filename(f1)['last_timestamp']
            first_timestamp = parse_local_sqlite3_filename(f2)['first_timestamp']
            if first_timestamp < last_timestamp:
                self._logger.warn(
                    '{}: timestamp overlapped {} < {} ({}, {})'.format(task, first_timestamp, last_timestamp, f1, f2)
                )
            last_id = parse_local_sqlite3_filename(f1)['last_id']
            first_id = parse_local_sqlite3_filename(f2)['first_id']
            if first_id <= last_id:
                self._logger.warn(
                    '{}: id overlapped {} < {} ({}, {})'.format(task, first_id, last_id, f1, f2)
                )

    def _drop_zero(self, executions):
        return [e for e in executions if (e._id not in (0, '0'))]

    def _to_params(self, execution):
        return (
            execution._id, execution.side, execution.price, execution.size, str(execution.timestamp),
            execution.buy_child_order_acceptance_id, execution.sell_child_order_acceptance_id
        )

    def _obtain_source_index(self, source_filenames):
        if not self._append_after:
            return 0

        n = 0
        for n, f in enumerate(source_filenames):
            source_timestamp = parse_local_sqlite3_filename(f)['first_timestamp']
            if self._append_after <= source_timestamp:
                break
        source_index = n - 1
        if source_index < 0:
            source_index = 0
        return source_index

    def _cleanup_destinations(self, task, destination, destination_filenames):
        if not self._append_after:
            return

        for filename in destination_filenames:
            destination_timestamp = parse_local_sqlite3_filename(filename)['last_timestamp']
            if self._append_after <= destination_timestamp:
                path_to_remove = os.path.join(destination, filename)
                os.remove(path_to_remove)
                self._logger.info('{}: clean up {}'.format(task, path_to_remove))

    def _build_execution(self, tpl):
        return Execution(
            _id=tpl[0], side=tpl[1], price=tpl[2], size=tpl[3], timestamp=np.datetime64(tpl[4], 'ns', utc=True),
            buy_child_order_acceptance_id=tpl[5], sell_child_order_acceptance_id=tpl[6]
        )

    def _get_first(self, conn):
        first_id, first_timestamp = conn.execute(
            'SELECT id, exec_date FROM executions ORDER BY id LIMIT 1'
        ).fetchall()[0]
        return dict(_id=first_id, timestamp=first_timestamp)

    def _get_last(self, conn):
        last_id, last_timestamp = conn.execute(
            'SELECT id, exec_date FROM executions ORDER BY id desc LIMIT 1'
        ).fetchall()[0]
        return dict(_id=last_id, timestamp=last_timestamp)

    def _build_insert_query(self, execution):
        return 'INSERT INTO executions VALUES ({}, "{}", {}, {}, "{}", "{}", "{}")'.format(
            execution._id, execution.side, execution.price, execution.size, execution.timestamp,
            execution.buy_child_order_acceptance_id, execution.sell_child_order_acceptance_id
        )

    def _parse_s3_objectname(self, object_name):
        # BTCJPY_bitflyer_executionboard-v1-2019-07-14T183518.1262655Z.log.xz
        name = os.path.basename(object_name)
        symbol, exchange, timestamp = name.split('_')
        timestamp = timestamp.split('-', maxsplit=2)[2].rstrip('.log.xz').rstrip('.tsv')
        timestamp = np.datetime64(revert_filename(timestamp), 'ns', utc=True)
        return dict(symbol=symbol, exchange=exchange, timestamp=timestamp)

    def _unparse_file_name(self, exchange, symbol, first_id, first_timestamp, last_id, last_timestamp,
                           postfix, origin='native'):
        # <exchange>_<symbol>_<id first>-<iso8601 first>_<id last>-<iso8601 last>_<origin if exist>.<postfix>
        if origin == 'native':
            origin = ''
        else:
            origin = '_{}'.format(origin)

        return safe_filename('{}_{}_{}-{}_{}-{}{}.{}'.format(
            exchange, symbol, first_id, first_timestamp, last_id, last_timestamp, origin, postfix
        ))


if __name__ == '__main__':
    from trade.log import get_logger

    _logger = get_logger(__name__, stream=sys.stdout)

    _exchange = 'bitflyer'
    _symbol = 'BTCJPY'
    # _symbol = 'FXBTCJPY'
    _append_after = '2019-10-13T21:28:42.305552500'

    _symbol_dir = os.path.join(config.PROJECT_ROOT, 'historical', _exchange, _symbol)
    if not os.path.exists(_symbol_dir):
        os.makedirs(_symbol_dir)

    updater = Updater(
        logger=_logger, symbol_dir=_symbol_dir,
        symbol=_symbol, exchange='bitflyer', timeunit='1minute', append_after=_append_after
    )

    # download log-lzma
    _process_name = 'log-lzma'
    updater.download_logs(_process_name)

    # extract log-lzma to log
    _pre_process_name = 'log-lzma'
    _process_name = 'log'
    updater.extract_logs(_pre_process_name, _process_name)

    # (deprecated: tsv to sqlite3)
    # _pre_process_name = 'tsv'
    # _process_name = 'sqlite3'
    # updater.tsv_to_sqlite3(_pre_process_name, _process_name)

    # log to sqlite3
    _pre_process_name = 'log'
    _process_name = 'sqlite3'
    updater.log_to_sqlite3(_pre_process_name, _process_name)

    # (deprecated: detect overlapped)
    _process_name = 'sqlite3'
    updater.detect_overlapped_sqlite3(_process_name)

    # reduce sqlite3 (newprices)
    # _pre_process_name = 'sqlite3'
    # _process_name = 'sqlite3-reduced-newprices1min'
    # updater.reduce_sqlite3_new_prices(_pre_process_name, _process_name)

    # reduce sqlite3 (ohlc1min)
    # _pre_process_name = 'sqlite3-reduced-newprices1min'
    _pre_process_name = 'sqlite3'
    _process_name = 'sqlite3-reduced-ohlc1min'
    updater.reduce_sqlite3_ohlc_4variables(_pre_process_name, _process_name)

    # ohlc_to_virtual_executions(
    #     'C:/Users/mizun/PycharmProjects/Trade/historical/working/tsv/BTCJPY_bitflyer_ohlcv1m-v1-1563045180.tsv',
    #     'C:/Users/mizun/PycharmProjects/Trade/historical/working/log/BTCJPY_bitflyer_ohlcv1m-v1-1563045180.log',
    #     '1minute',
    #     high_offset=pd.to_timedelta('1minute') - pd.to_timedelta('2nanosecond'),
    #     low_offset=pd.to_timedelta('1minute') - pd.to_timedelta('2nanosecond'),
    #     close_offset=pd.to_timedelta('1minute') - pd.to_timedelta('1nanosecond'),
    # )
