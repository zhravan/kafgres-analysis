#!/usr/bin/env python3
import argparse, json, statistics
from pathlib import Path

def load(root):
    rows=[]
    for p in root.glob("case-*.json"):
        try: rows.append(json.loads(p.read_text()))
        except Exception: pass
    return rows

def med(vals):
    return statistics.median([v for v in vals if v is not None]) if any(v is not None for v in vals) else None

def main():
    p=argparse.ArgumentParser(); p.add_argument("--results",required=True); p.add_argument("--output",required=True)
    a=p.parse_args(); root=Path(a.results)
    systems={s:load(root/s) for s in ["kafka","kafgres"]}
    keys=set()
    for rows in systems.values():
        for r in rows: keys.add((r["message_size"],r["partitions"],r["producers"],r["consumers"]))
    lines=["# Kafka vs Kafgres Benchmark Results","","Run-level medians are reported; no score or overall ranking is produced.","",
           "| Size | Partitions | Producers | Consumers | Kafka ack rec/s | Kafgres ack rec/s | Kafgres/Kafka | Kafka p99 ms | Kafgres p99 ms |",
           "|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    summary=[]
    for k in sorted(keys):
        vals={}
        for s in systems:
            rs=[r for r in systems[s] if (r["message_size"],r["partitions"],r["producers"],r["consumers"])==k]
            vals[s]=(med([r["producer_acked_records_per_sec"] for r in rs]),med([r["latency_ms"]["p99"] for r in rs]),len(rs))
        kr, kp, kn=vals["kafka"]; gr, gp, gn=vals["kafgres"]
        ratio=(gr/kr*100) if kr else None
        lines.append(f"| {k[0]} | {k[1]} | {k[2]} | {k[3]} | {kr:.2f} | {gr:.2f} | {(ratio or 0):.2f}% | {kp:.3f} | {gp:.3f} |")
        summary.append({"message_size":k[0],"partitions":k[1],"producers":k[2],"consumers":k[3],"kafka_records_per_sec":kr,"kafgres_records_per_sec":gr,"kafgres_as_pct_of_kafka":ratio,"kafka_p99_ms":kp,"kafgres_p99_ms":gp,"kafka_runs":kn,"kafgres_runs":gn})
    (root/"summary.json").write_text(json.dumps(summary,indent=2))
    Path(a.output).write_text("\n".join(lines)+"\n")
    print("\n".join(lines))

if __name__=="__main__": main()
