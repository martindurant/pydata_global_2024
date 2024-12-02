import datetime
import ipaddress
import json
import re

import fsspec
import awkward as ak


def hdfs():
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


def _dep(s):
    parts = s.split()
    if len(parts) == 1:
        return {"name": parts[0], "op": None, "versions": []}
    if len(parts) != 2:
        # unknown (to me) format
        return None
    name, rest = parts
    match = re.match(r"([<>=]+)([\d.]+)", rest)
    if match:
        op, num = match.groups()
        nums = [int(_) for _ in num.split(".") if _]
        return {"name": name, "op": op, "versions": nums}
    return None


def repodata():
    remote = "https://repo.anaconda.com/pkgs/main/linux-64/repodata.json" # bz2 version much smaller
    with fsspec.open(remote, "rt") as f:
        out = json.load(f)
    packages = out["packages.conda"]
    o = []
    for file, p in packages.items():
        p = p.copy()  # don't corrupt original, if need to rerun
        p["depends"] = [_dep(_) for _ in p["depends"]]
        p["timestamp"] = datetime.datetime.fromtimestamp(p["timestamp"] // 1000) if "timestamp" in p else None
        p["filename"] = file
        o.append(p)
    arr = ak.Array(o)
    return arr
