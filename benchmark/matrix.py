#!/usr/bin/env python3
import argparse, itertools, json, subprocess, sys
from pathlib import Path

def cases(profile):
    if profile=="ci":
        return [
          (1024,1,1,1),(1024,3,1,1),(1024,12,1,1),
          (10240,1,1,1),(10240,3,1,1),(10240,12,1,1),
          (102400,1,1,1),(102400,3,1,1),(102400,12,1,1),
          (1024,3,4,4),(1024,3,16,16)
        ]
    sizes=[100,1024,10240,102400]; parts=[1,3,6,12]; conc=[1,4,16]
    return list(itertools.product(sizes,parts,conc,conc))

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--bootstrap",required=True); p.add_argument("--system",required=True)
    p.add_argument("--output-dir",required=True); p.add_argument("--container")
    p.add_argument("--profile",choices=["ci","full"],default="ci")
    p.add_argument("--duration",type=float,default=20); p.add_argument("--warmup",type=float,default=5)
    p.add_argument("--repetitions",type=int,default=3)
    a=p.parse_args(); out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    root=Path(__file__).resolve().parent
    caseset=cases(a.profile)
    manifest={"profile":a.profile,"cases":len(caseset),"repetitions":a.repetitions}
    (out/"manifest.json").write_text(json.dumps(manifest,indent=2))
    for case_i,(size,parts,prods,cons) in enumerate(caseset):
        for rep in range(1,a.repetitions+1):
            name=f"case-{case_i:03d}-rep-{rep:02d}.json"
            cmd=[sys.executable,str(root/"benchmark.py"),"--bootstrap",a.bootstrap,"--system",a.system,
                 "--output",str(out/name),"--message-size",str(size),"--partitions",str(parts),
                 "--producers",str(prods),"--consumers",str(cons),"--duration",str(a.duration),"--warmup",str(a.warmup)]
            if a.container: cmd += ["--container",a.container]
            print("RUN"," ".join(cmd),flush=True)
            subprocess.run(cmd,check=True)
if __name__=="__main__": main()
