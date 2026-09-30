#!/usr/bin/env python3
import argparse, json, os, re, struct, subprocess, threading, time, uuid
from pathlib import Path
from statistics import quantiles
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

def bytes_value(s):
    m=re.match(r"^([0-9.]+)([KMG]i?B)?$", str(s))
    if not m: return int(s)
    n=float(m.group(1)); u=m.group(2) or ""
    mult={"":1,"KB":1000,"MB":1000**2,"GB":1000**3,"KiB":1024,"MiB":1024**2,"GiB":1024**3}[u]
    return int(n*mult)

def parse_args():
    p=argparse.ArgumentParser()
    p.add_argument("--bootstrap",required=True); p.add_argument("--system",required=True)
    p.add_argument("--output",required=True); p.add_argument("--message-size",type=bytes_value,default=1024)
    p.add_argument("--partitions",type=int,default=3); p.add_argument("--producers",type=int,default=1)
    p.add_argument("--consumers",type=int,default=1); p.add_argument("--duration",type=float,default=10)
    p.add_argument("--warmup",type=float,default=3); p.add_argument("--container")
    p.add_argument("--topic-prefix",default="benchmark"); return p.parse_args()

def create_topic(bootstrap, topic, partitions):
    admin=AdminClient({"bootstrap.servers":bootstrap})
    fs=admin.create_topics([NewTopic(topic,num_partitions=partitions,replication_factor=1)])
    fs[topic].result(timeout=30)

def docker_stats(container):
    if not container: return {}
    try:
        r=subprocess.run(["docker","stats","--no-stream","--format","{{json .}}",container],
                         capture_output=True,text=True,timeout=10,check=True)
        return json.loads(r.stdout.strip() or "{}")
    except Exception as e:
        return {"error":str(e)}

def resource_sampler(container, stop, samples):
    while not stop.is_set():
        samples.append({"ts_ns":time.time_ns(),"docker":docker_stats(container)})
        stop.wait(1.0)

def percentile(values,p):
    if not values: return None
    if len(values)==1: return float(values[0])
    return float(quantiles(values,n=1000,method="inclusive")[int(p*10)-1])

def main():
    a=parse_args()
    Path(a.output).parent.mkdir(parents=True,exist_ok=True)
    topic=f"{a.topic_prefix}-{uuid.uuid4().hex[:12]}"
    create_topic(a.bootstrap,topic,a.partitions)
    samples=[]; sampler_stop=threading.Event()
    sampler=threading.Thread(target=resource_sampler,args=(a.container,sampler_stop,samples),daemon=True); sampler.start()

    produced=consumed=0
    delivery_errors=[]; latency=[]; lock=threading.Lock()
    measurement_start=0; measurement_end=0; stop_producers=threading.Event(); measure_start_event=threading.Event()\n    consumer_group=f"bench-{uuid.uuid4().hex}"
    stop_consumers=threading.Event()

    def delivery(err,msg):
        nonlocal produced
        if err:
            with lock: delivery_errors.append(str(err))
            return
        try: sent=struct.unpack(">Q",msg.value()[:8])[0]
        except Exception: return
        with lock:
            if measurement_start <= sent <= measurement_end:
                produced += 1

    def producer_worker(idx):
        conf={"bootstrap.servers":a.bootstrap,"acks":"all","enable.idempotence":True,
              "compression.type":"none","linger.ms":5,"batch.size":131072,
              "client.id":f"bench-{a.system}-p{idx}"}
        prod=Producer(conf); seq=0
        warm_end=time.monotonic()+a.warmup
        while time.monotonic()<warm_end:
            payload=struct.pack(">QQ",time.time_ns(),seq)+b"x"*max(0,a.message_size-16); seq+=1
            try: prod.produce(topic,value=payload,on_delivery=delivery); prod.poll(0)
            except BufferError: prod.poll(0.01)
        measure_start_event.wait()\n        while not stop_producers.is_set():
            payload=struct.pack(">QQ",time.time_ns(),seq)+b"x"*max(0,a.message_size-16); seq+=1
            try: prod.produce(topic,value=payload,on_delivery=delivery); prod.poll(0)
            except BufferError: prod.poll(0.01)
        prod.flush(30)

    def consumer_worker(idx):
        nonlocal consumed
        conf={"bootstrap.servers":a.bootstrap,"group.id":consumer_group,
              "auto.offset.reset":"earliest","enable.auto.commit":False,
              "fetch.min.bytes":1,"fetch.max.wait.ms":10,"max.partition.fetch.bytes":1048576,
              "client.id":f"bench-{a.system}-c{idx}"}
        c=Consumer(conf); c.subscribe([topic])
        idle=0
        while not stop_consumers.is_set():
            msg=c.poll(1.0)
            if msg is None: idle+=1; continue
            idle=0
            if msg.error(): 
                if msg.error().code() == KafkaError._PARTITION_EOF: continue
                continue
            try: sent=struct.unpack(">Q",msg.value()[:8])[0]
            except Exception: continue
            now=time.time_ns()
            with lock:
                if measurement_start <= sent <= measurement_end:
                    latency.append(max(0,now-sent)/1e6); consumed+=1
        c.close()

    consumers=[threading.Thread(target=consumer_worker,args=(i,),daemon=True) for i in range(a.consumers)]
    for t in consumers: t.start()
    time.sleep(2)
    producers=[threading.Thread(target=producer_worker,args=(i,),daemon=True) for i in range(a.producers)]
    for t in producers: t.start()
    time.sleep(a.warmup)
    measurement_start=time.time_ns()
    measurement_end=measurement_start+int(a.duration*1e9)
    measure_start_event.set()
    time.sleep(a.duration)
    stop_producers.set()
    for t in producers: t.join(timeout=60)
    time.sleep(min(10,max(2,a.duration*0.2)))
    stop_consumers.set()
    for t in consumers: t.join(timeout=15)
    sampler_stop.set(); sampler.join(timeout=3)

    elapsed=a.duration
    result={
      "schema_version":1,"system":a.system,"topic":topic,"bootstrap":a.bootstrap,
      "message_size":a.message_size,"partitions":a.partitions,"producers":a.producers,
      "consumers":a.consumers,"duration_s":a.duration,"warmup_s":a.warmup,
      "producer_acked_records":produced,"consumer_records":consumed,
      "producer_acked_records_per_sec":produced/elapsed,"consumer_records_per_sec":consumed/elapsed,
      "latency_ms":{"count":len(latency),"p50":percentile(latency,50),"p95":percentile(latency,95),
                    "p99":percentile(latency,99),"p99_9":percentile(latency,99.9),
                    "min":min(latency) if latency else None,"max":max(latency) if latency else None},
      "delivery_errors":delivery_errors[:20],"resource_samples":samples,
      "timestamp_utc":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())
    }
    Path(a.output).write_text(json.dumps(result,indent=2))
    print(json.dumps({k:result[k] for k in ["system","message_size","partitions","producers","consumers","producer_acked_records_per_sec","consumer_records_per_sec","latency_ms"]},indent=2))

if __name__=="__main__": main()
