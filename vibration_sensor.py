import os
import json
import random
import uuid
import time
from datetime import datetime, timedelta

# Configuration
sensor_type = "vibration"
unit = "Hz"
value_range = (0.0, 10.0)
output_dir = f"data_lake/raw/{sensor_type}"
os.makedirs(output_dir, exist_ok=True)

sites = ["Site A", "Site B", "Site C"]
machines = ["Machine 01", "Machine 02", "Machine 03"]

start_date = datetime(2025, 5, 10)
num_days = 10
measures_per_day = 100

# Pour chaque jour simulé
for day in range(num_days):
    print(f"📆 Simulation du jour : {start_date.date() + timedelta(days=day)}")

    current_time = start_date + timedelta(days=day, hours=0)

    for i in range(measures_per_day):
        # Valeurs simulées
        data = {
            "sensor_id": str(uuid.uuid4()),
            "type": sensor_type,
            "value": round(random.uniform(*value_range), 2),
            "unit": unit,
            "site": random.choice(sites),
            "machine": random.choice(machines),
            "timestamp": current_time.isoformat()
        }

        # Sauvegarde
        filename = f"{data['sensor_id']}.json"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w") as f:
            json.dump(data, f)

        print(f"✅ Mesure générée à {data['timestamp']}")
        
        # Pause réelle (1 à 3 secondes)
        pause = random.randint(1, 3)
        time.sleep(pause)

        # Avancer l'heure simulée de pause secondes
        current_time += timedelta(seconds=pause)
