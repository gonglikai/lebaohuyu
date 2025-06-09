import pandas as pd
import random
import csv
import gc
import os
import time
from datetime import datetime, timedelta

# 配置
NUM_PLAYERS = 100_000
SIM_DAYS = 180
MIN_EVENTS = 50
MAX_EVENTS = 200
EVENT_TYPES = ["LevelComplete", "InAppPurchase", "SocialInteraction"]
COUNTRIES = ["USA", "China", "Singapore", "Brazil", "Japan", "Germany", "India", "UK", "France", "Canada"]
DEVICES = ["Android", "iOS", "PC"]
random.seed(42)

def gen_event_details(t):
    if t == "LevelComplete":
        return f"Level:{random.randint(1,100)},Score:{random.randint(1000,50000)}"
    if t == "InAppPurchase":
        return f"Amount:${random.choice([0.99,2.99,4.99,9.99,19.99,49.99,99.99])}"
    if t == "SocialInteraction":
        return f"Action:{random.choice(['JoinGuild','SendMessage','AddFriend','ShareScore'])}"
    return ""

def gen_player_events(pid):
    n = random.randint(MIN_EVENTS, MAX_EVENTS)
    max_s = SIM_DAYS * 86400
    times = sorted(random.randint(0, max_s) for _ in range(n))
    events = []
    for i, ts in enumerate(times):
        if i == 0:
            et = "Login"
        elif i == n - 1:
            et = "Logout"
        else:
            et = random.choice(EVENT_TYPES)
        events.append((pid, ts, et, gen_event_details(et)))
    return events

def simulate(outfile="game_events.csv"):
    # 删除旧文件
    if os.path.exists(outfile):
        os.remove(outfile)
    f = open(outfile, "w", newline="", encoding="utf-8")
    w = csv.writer(f)
    w.writerow(["event_id","player_id","timestamp","type","details","device","country"])

    counter = 0
    start = time.time()
    for batch_start in range(0, NUM_PLAYERS, 1000):
        batch_end = min(batch_start + 1000, NUM_PLAYERS)
        for i in range(batch_start, batch_end):
            pid = f"P{100000+i}"
            dev = random.choice(DEVICES)
            loc = random.choice(COUNTRIES)
            for _, ts, et, det in gen_player_events(pid):
                eid = f"E{counter}"
                counter += 1
                dt = (datetime(2023,1,1) + timedelta(seconds=ts))\
                     .strftime("%Y-%m-%d %H:%M:%S")
                w.writerow([eid, pid, dt, et, det, dev, loc])
        gc.collect()
        print(f"Processed players {batch_start}–{batch_end-1}")
    f.close()
    print(f"Done: {counter} events in {time.time()-start:.1f}s")

if __name__ == "__main__":
    simulate()
