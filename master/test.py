from master import Master

DATA_SIZE  = 10_000
SCRIPT     = "program.py"

master = Master()
slaves = master.discover()
slave_ips = list(slaves.keys()) # все ip
slave_ips_pool = slave_ips # пул свободных ip

Ids=[337540,336780,337450,338290]

tasks = []

for station in Ids:
    start = station
    # end   = start + chunk if i < len(slave_ips) - 1 else DATA_SIZE
    end = '0'
    tasks.append({"params": f"-start {start} -end {end}"})

results = master.run(script_path=SCRIPT, tasks=tasks)