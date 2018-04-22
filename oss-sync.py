#!/usr/bin/env python
# -*- coding: utf8 -*-


import os
import sys
import json
import hashlib
import platform

import oss2

debug = False

sep = os.path.sep

def oss_sync(config):
    assert config.get("access_key_id")
    assert config.get("access_key_secret")
    assert config.get("bucket")
    assert config.get("endpoint")
    assert config.get("local")
    assert config.get("remote")
    assert not config.get("local").endswith(sep)
    assert not config.get("remote").endswith('/')

    auth = oss2.Auth(config["access_key_id"], config["access_key_secret"])
    bucket = oss2.Bucket(auth, config["endpoint"], config["bucket"])

    def rfn_to_local_afn(rfn):
        return ('%s/%s' % (config["local"], rfn)).replace('/', sep)
    def rfn_to_remote_afn(rfn):
        return '%s/%s' % (config["remote"], rfn)

    # scan local
    local = {}
    local_filesmap_prefix = '~%s.oss_sync' % sep
    for path, dirs, files in os.walk(config["local"]):
        if path.replace(config["local"], '~').startswith(local_filesmap_prefix):
            continue
        for fn in files:
            afn = '%s%s%s' % (path, sep, fn)
            rfn = afn.replace(config["local"]+sep, '').replace('\\', '/')
            mod = os.stat(afn).st_mtime
            local[rfn] = (mod, )
    if debug:
        print ('[local]')
        print (local)

    # get local map
    local_filesmap_dir = '%s%s.oss_sync' % (config["local"], sep)
    local_filesmap = '%s%slfs.json' % (local_filesmap_dir, sep)
    local2 = json.load(open(local_filesmap)) \
        if os.path.exists(local_filesmap) else {}
    if debug:
        print ('[local2]')
        print (local2)

    # # scan remote
    # remote = {}
    # fin = False
    # marker = None
    # while not fin:
    #     osr = bucket.list_objects(prefix=config["remote"]+'/',
    #         marker=marker)
    #     marker = osr.next_marker
    #     if not marker:
    #         fin = True
    #     for o in osr.object_list:
    #         rfn = o.key.replace(config["remote"]+'/', '')
    #         if not rfn or rfn.startswith('.oss_sync'):
    #             continue
    #         remote[rfn] = ()
    # if debug:
    #     print '[remote]'
    #     print remote

    # get remote map
    remote_filesmap = '%s/.oss_sync/rfs.json' % config["remote"]
    try:
        rfs = bucket.get_object(remote_filesmap)
        remote2 = json.load(rfs)
    except oss2.exceptions.NoSuchKey:
        remote2 = {}
    if debug:
        print ('[remote2]')
        print (remote2)

    def get_file_hash(rfn):
        f = open(rfn_to_local_afn(rfn), 'rb')
        obj = hashlib.md5()
        obj.update(f.read())
        return obj.hexdigest()

    # compute local changes
    local_add = {}
    local_mod = {}
    local_del = {}
    for k, v in local.items():
        if k not in local2:
            local_add[k] = (v[0], get_file_hash(k))
        else:
            if v[0] != local2[k][0]:
                new_hash = get_file_hash(k)
                if new_hash != local2[k][1]:
                    local_mod[k] = (v[0], new_hash)
                else:
                    local2[k][0] = v[0]
    for k, v in local2.items():
        if k not in local:
            local_del[k] = v
    if debug:
        print ('[local_add]')
        print (local_add)
        print ('[local_mod]')
        print (local_mod)
        print ('[local_del]')
        print (local_del)

    # compute remote changes
    remote_add = {}
    remote_mod = {}
    remote_del = {}
    for k, v in remote2.items():
        if k not in local2:
            remote_add[k] = v
        else:
            if v[1] != local2[k][1]:
                remote_mod[k] = v
    for k, v in local2.items():
        if k not in remote2:
            remote_del[k] = v
    if debug:
        print ('[remote_add]')
        print (remote_add)
        print ('[remote_mod]')
        print (remote_mod)
        print ('[remote_del]')
        print (remote_del)

    def sync_filesmap(local_dict=None, remote_dict=None):
        if remote_dict != None:
            bucket.put_object(remote_filesmap,
                json.dumps(remote_dict, indent=2))
        if local_dict != None:
            if not os.path.exists(local_filesmap_dir):
                os.mkdir(local_filesmap_dir)
                if 'Windows' in platform.system():
                    os.system("attrib +h %s" % local_filesmap_dir)
            json.dump(local_dict, open(local_filesmap, 'w'), indent=2)

    #sync_filesmap(local2, remote2)
    sync_filesmap(local2, None)

    def remote_to_local(rfn):
        r_afn = rfn_to_remote_afn(rfn)
        l_afn = rfn_to_local_afn(rfn)
        l_dir = os.path.dirname(l_afn)
        if not os.path.exists(l_dir):
            os.makedirs(l_dir)
        bucket.get_object_to_file(r_afn, l_afn)

    def local_to_remote(rfn):
        r_afn = rfn_to_remote_afn(rfn)
        l_afn = rfn_to_local_afn(rfn)
        bucket.put_object_from_file(r_afn, l_afn)

    for k, v in remote_add.items():
        print ('[R-Add] Downloading %s ...' % k)
        assert k not in local_add
        remote_to_local(k)
        local2[k] = v
        sync_filesmap(local2, None)

    for k, v in remote_mod.items():
        print ('[R-Mod] Downloading %s ...' % k)
        assert k not in local_mod
        remote_to_local(k)
        local2[k] = v
        sync_filesmap(local2, None)

    for k, v in remote_del.items():
        print ('[R-Del] Deleting %s ...' % k)
        assert k not in local_del
        os.remove(rfn_to_local_afn(k))
        del local2[k]
    sync_filesmap(local2, None)

    for k, v in local_add.items():
        print ('[L-Add] Uploading %s ...' % k)
        local_to_remote(k)
        local2[k] = v
        remote2[k] = v
        sync_filesmap(local2, remote2)

    for k, v in local_mod.items():
        print ('[L-Mod] Uploading %s ...' % k)
        local_to_remote(k)
        local2[k] = v
        remote2[k] = v
        sync_filesmap(local2, remote2)

    for k, v in local_del.items():
        print ('[L-Del] Deleting %s ...' % k)
        del local2[k]
        del remote2[k]
    sync_filesmap(local2, remote2)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print ('Usage: python oss-sync.py <config>')
        sys.exit()

    config = json.load(open(sys.argv[1]))
    oss_sync(config)
