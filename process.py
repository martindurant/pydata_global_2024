import datetime
import re

import awkward as ak
import ipaddress
import regex

fn = "node_logs/hadoop-hdfs-namenode-mesos-01.log"
ip_x = regex.compile(r"(\d+\.\d+\.\d+\.\d+):")
block_x = re.compile(r".*blk_(\d+_\d+)")
len_iso = len("2016-10-22 13:28:23,949")

files = {}
blocks = {}
b = ak.ArrayBuilder()

with (open(fn) as f):
    for n, line in enumerate(f):
        time = line[:len_iso].replace(" ", "T").replace(",", ".")
        if "BlockStateChange" in line and "addStoredBlock" in line:
            ips = ip_x.findall(line)
            b_ips = [ipaddress.IPv4Address(i).packed for i in ips]
            block = block_x.match(line).groups()[0]
            # IPs will become bytestrings, but should be fixed-width
            blocks[block] = {"ip": b_ips, "time": datetime.datetime.fromisoformat(time)}
        elif "hdfs.StateChange" in line:
            if "completeFile" in line:
                fname = line.split("completeFile: ")[1].split()[0]
                data = files.pop(fname, None)
                if data is None:
                    continue
                b.begin_record()
                b.field("path").string(fname)
                b.field("time").datetime(time)
                b.field("blocks").begin_list()
                for block_id in data:
                    b.append(blocks.pop(block_id, {"ip": None, "time": None}))
                b.end_list()
                b.end_record()
            else:
                fname = line.rsplit(None, 2)[2]
                m = block_x.match(line)
                if m:
                    block = m.groups()[0]
                    files.setdefault(fname, []).append(block)


ak.to_parquet(b.snapshot(), "out.parquet", list_to32=True, extensionarray=False)
