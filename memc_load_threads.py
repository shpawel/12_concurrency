#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache

from multiprocessing import cpu_count, Pool as ThreadPool
from Queue import Queue, Empty
import threading
import time

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    # os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc = memcache.Client([memc_addr])
            memc.set(key, packed)
    except Exception, e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def worker(queue, devices, dry, r_lock, w_lock, s_event, statuses):
    """
    Воркер для обработки строк

    :param Queue.Queue queue: очередь со строками
    :param dict[str] devices: словать устройств и адресов
    :param threading.RLock r_lock: блокировщик чтения
    :param threading.RLock w_lock: блокировщих записи
    :param threading.Event s_event: событи остановки работы потока
    :param dict[str] statuses: статусы обработки строк
    :return:
    """
    while True:
        if s_event.is_set():
            logging.info('Exit from thread')
            break
        try:
            next_line = queue.get_nowait()
        except Empty as e:
            pass
            continue
        else:
            try:
                appsinstalled = parse_appsinstalled(next_line)
                if not appsinstalled:
                    with r_lock:
                        statuses['errors'] += 1
                    continue

                memc_addr = devices.get(appsinstalled.dev_type)
                if not memc_addr:
                    with r_lock:
                        statuses['errors'] += 1
                        logging.error("Unknow device type: %s" % appsinstalled.dev_type)
                        continue

                with w_lock:
                    ok = insert_appsinstalled(memc_addr, appsinstalled, dry)

                with r_lock:
                    if ok:
                        statuses['processed'] += 1
                    else:
                        statuses['errors'] += 1
            finally:
                queue.task_done()


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    read_lock = threading.RLock()
    write_lock = threading.RLock()
    stop_event = threading.Event()
    queue = Queue()
    thread_pool = []

    statuses = dict(
        processed=0,
        errors=0
    )

    for n in range(cpu_count()):
        thread = threading.Thread(
            target=worker,
            args=(queue, device_memc, options.dry, read_lock, write_lock, stop_event, statuses)
        )
        thread.setDaemon(True)
        thread.start()
        thread_pool.append(thread)

    for fn in glob.iglob(options.pattern):
        fd = gzip.open(fn)
        statuses['processed'] = 0
        statuses['errors'] = 0
        i = 0
        for line in fd:
            i += 1
            line = line.strip()
            if not line:
                continue
            queue.put(line)
            if i >= 50000:
                break

        queue.join()

        err_rate = float(statuses['errors']) / statuses['processed']
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        # dot_rename(fn)

    stop_event.set()

    for thread in thread_pool:
        thread.join()


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log,
                        level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(threadName) -10s %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        start = time.time()
        main(opts)
        logging.info('Завершено за %s', time.time()-start)
    except Exception, e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
