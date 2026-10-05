"""Create deterministic column-wise shards with a common trip_id."""
import csv, hashlib, sys
from pathlib import Path

src = Path(sys.argv[1] if len(sys.argv)>1 else "data/raw/uber-raw-data-apr14.csv")
out = Path(sys.argv[2] if len(sys.argv)>2 else "data/cluster_shards")
out.mkdir(parents=True, exist_ok=True)
files = {
    "master": out/"master_datetime.csv",
    "worker1": out/"worker1_lat.csv",
    "worker2": out/"worker2_lon_base.csv",
}
handles = {k: v.open("w", encoding="utf-8", newline="") for k,v in files.items()}
writers = {
    "master": csv.writer(handles["master"]),
    "worker1": csv.writer(handles["worker1"]),
    "worker2": csv.writer(handles["worker2"]),
}
for k,w in writers.items():
    w.writerow(["trip_id", "Date/Time"] if k=="master" else ["trip_id", "Lat"] if k=="worker1" else ["trip_id", "Lon", "Base"])
try:
    with src.open("r", encoding="utf-8", newline="") as fh:
        for n,row in enumerate(csv.DictReader(fh),1):
            identity=f"{n}|{row['Date/Time'].strip()}|{row['Lat'].strip()}|{row['Lon'].strip()}|{row['Base'].strip()}"
            trip="uber-"+hashlib.sha256(identity.encode()).hexdigest()[:20]
            writers["master"].writerow([trip,row["Date/Time"]])
            writers["worker1"].writerow([trip,row["Lat"]])
            writers["worker2"].writerow([trip,row["Lon"],row["Base"]])
finally:
    for h in handles.values(): h.close()
print(f"Created 3 column shards under {out}")
