import time, datetime, requests, json, os, sqlite3, re
from discord_webhook import DiscordWebhook
from datetime import timezone
from math import floor


headers = {"Accept": "application/json"}
url = "https://data.vatsim.net/v3/vatsim-data.json"

with open("config.json") as f:
    config = json.load(f)


class VatsimScraper:
    def __init__(self, save_directory, log_directory, webhook_url):
        self.save_directory = save_directory
        self.log_directory = log_directory
        if not os.path.exists(self.save_directory):
            os.mkdir(self.save_directory)
        if not os.path.exists(os.path.join(self.save_directory, "data")):
            os.mkdir(os.path.join(self.save_directory, "data"))
        if not os.path.exists(self.log_directory):
            os.mkdir(self.log_directory)
        self.active = True
        self.actv_conns = {}
        self.disc_conns = []
        self.webhook_url = webhook_url
        self.update(first=True)

    @staticmethod
    def is_same_cid_and_callsign(i1, i2):
        return i1["cid"] == i2["cid"] and i1["callsign"] == i2["callsign"]

    @staticmethod
    def is_same_flight(i1, i2):
        if not VatsimScraper.is_same_cid_and_callsign(i1, i2):
            return False
        fp1 = i1["flight_plan"]
        fp2 = i2["flight_plan"]
        return fp1["departure"] == fp2["departure"] and fp1["arrival"] == fp2["arrival"]

    def new_connection(self, data, status="normal"):
        if str(data["cid"]) in self.actv_conns:
            return "Error: Already active connection with this CID"
        if "flight_plan" in data:
            self.actv_conns[str(data["cid"])] = {
                "cid": data["cid"],
                "callsign": data["callsign"],
                "flight_plan": data["flight_plan"],
                "logon_time": data["logon_time"][:19],
                "last_updated": data["last_updated"][:19],
                "record_status": status,
                "airborne_time": -1,
                "lowest_alt": data["altitude"],
            }
        else:
            self.actv_conns[str(data["cid"])] = {
                "cid": data["cid"],
                "callsign": data["callsign"],
                "facility": data["facility"],
                "rating": data["rating"],
                "logon_time": data["logon_time"][:19],
                "last_updated": data["last_updated"][:19],
                "record_status": status,
            }
        self.log(f"New connection: {data['callsign']}")

    def end_connection(self, cid, status="normal"):
        data = self.actv_conns.pop(str(cid))
        if 'flight_plan' in data:
            if data["airborne_time"] == -1:
                return
        data["record_status"] = (
            status if data["record_status"] == "normal" else data["record_status"]
        )
        self.disc_conns.append(data)
        self.log(f"Ended connection: {cid} / {data['callsign']}")

    def update_connection(self, data):
        if "flight_plan" in data:
            if (
                self.actv_conns[str(data["cid"])]["airborne_time"] == -1
                and self.actv_conns[str(data["cid"])]["lowest_alt"] + 100
                < data["altitude"]
            ):
                self.actv_conns[str(data["cid"])]["airborne_time"] = floor(
                    datetime.datetime.now(timezone.utc).timestamp() + 0.5
                )
        self.actv_conns[str(data["cid"])]["last_updated"] = data["last_updated"][:19]

    def update(self, first=False):
        try:
            status = "scrapper_started" if first else "normal"
            response = requests.request("GET", url, headers=headers).json()
            conn_updated = 0
            pilots_data = []
            for j in response["pilots"]:
                if (
                    j is not None
                    and j["flight_plan"] is not None
                    and sum(
                        [
                            j["flight_plan"][i][:2] in ["RK", "ZK"]
                            for i in ["departure", "arrival"]
                        ]
                    )
                ):
                    pilots_data.append(j)
            for p_data in pilots_data:
                if str(p_data["cid"]) not in self.actv_conns:
                    self.new_connection(p_data, status=status)
                elif not VatsimScraper.is_same_flight(
                    p_data, self.actv_conns[str(p_data["cid"])]
                ):
                    self.end_connection(str(p_data["cid"]))
                    self.new_connection(p_data, status=status)
                else:
                    self.update_connection(p_data)
                conn_updated += 1

            controllers_data = [
                i for i in response["controllers"] if i is not None and i['facility'] != 0 and i["callsign"][:2] in ["RK", "ZK"]
            ]
            for c_data in controllers_data:
                if str(c_data["cid"]) not in self.actv_conns:
                    self.new_connection(c_data, status=status)
                elif not VatsimScraper.is_same_cid_and_callsign(
                    c_data, self.actv_conns[str(c_data["cid"])]
                ):
                    self.end_connection(str(c_data["cid"]))
                    self.new_connection(c_data, status=status)
                else:
                    self.update_connection(c_data)
                conn_updated += 1
            actv_cids = [str(i["cid"]) for i in pilots_data + controllers_data]
            for cid in list([i for i in self.actv_conns]):
                if not cid in actv_cids:
                    conn_updated += 1
                    self.end_connection(str(cid))
            return {
                "ok": True,
                "data": f"Connections updated: {conn_updated}",
            }
        except Exception as e:
            # raise e  # disable error handling for test
            DiscordWebhook(
                url=self.webhook_url,
                content=f"VATSIM Scraper Error: {str(e)}",
            ).execute()
            return {"ok": False, "error": str(e)}

    def run(self):
        now = datetime.datetime.now(timezone.utc)
        day = now.day
        hour = now.hour
        while self.active:
            time.sleep(60)
            result = self.update()
            if result["ok"]:
                self.log(f"Update successful: {result['data']}")
            else:
                self.log(f"Update failed: {result['error']}")
            now = datetime.datetime.now(timezone.utc)
            if hour != now.hour:
                self.dump_data()
                self.log(f"New hour: data for {hour} dumped")
                hour = now.hour
            if day != now.day:
                os.chdir(self.save_directory)
                os.system("git add .")
                os.system(
                    f'git commit -m "Automatic daily commit by VATSIM Scraper: {day}"'
                )
                os.system("git push origin main")
                os.chdir(os.path.dirname(os.path.abspath(__file__)))
                self.log(f"New day: github pushed for day {day}")
                day = now.day

    def stop(self):
        for i in list(self.actv_conns.keys()):
            self.end_connection(i, status="scraper_stopped")
        self.active = False
        self.dump_data()
        self.log(f"Scraper stopped at {datetime.datetime.now(timezone.utc)}")

    def log(self, message):
        today = datetime.datetime.now(timezone.utc).strftime("%y%m%d")
        with open(os.path.join(self.log_directory, today + ".json"), "a") as log_file:
            log_file.write(f"{datetime.datetime.now(timezone.utc)}: {message}\n")
        print(f"{datetime.datetime.now(timezone.utc)}: {message}")

    def dump_data(self):
        dbs = {}
        cVr = re.compile(r"/[Vv]/")
        cRr = re.compile(r"/[Rr]/")
        pilots_dumped = 0
        controllers_dumped = 0
        for i in self.disc_conns:
            if "flight_plan" in i:
                save_ts = datetime.datetime.fromtimestamp(i["airborne_time"])
            else:
                save_ts = datetime.datetime.strptime(
                    i["logon_time"], "%Y-%m-%dT%H:%M:%S"
                )
            if (day := save_ts.strftime("%y%m%d")) not in dbs:
                dbs[day] = (w := sqlite3.connect(
                    os.path.join(self.save_directory, "data", f"{day}.db")
                ), w.cursor())
                dbs[day][1].execute(
                    """CREATE TABLE IF NOT EXISTS pilots
                                  (callsign TEXT, departure TEXT, arrival TEXT, alternate TEXT, acft TEXT,
                                   logon_time INTEGER, last_updated INTEGER,
                                   record_normal INTEGER, airborne_time INTEGER,
                                   route TEXT, comm INTEGER, flight_rule INTEGER)"""
                )
                dbs[day][1].execute(
                    """CREATE TABLE IF NOT EXISTS controllers (callsign TEXT, facility INTEGER, rating INTEGER, record_normal INTEGER, logon_time INTEGER, last_updated INTEGER)"""
                )
                dbs[day][0].commit()
            if "flight_plan" in i:
                if cVr.search(i["flight_plan"]["remarks"]):
                    fr = 2
                elif cRr.search(i["flight_plan"]["remarks"]):
                    fr = 1
                else:
                    fr = 0

                dbs[day][1].execute(
                    """INSERT INTO pilots (callsign, departure, arrival, alternate, acft,
                                            logon_time, last_updated,
                                            record_normal, airborne_time,
                                            route, comm, flight_rule) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        i["callsign"],
                        i["flight_plan"]["departure"],
                        i["flight_plan"]["arrival"],
                        i["flight_plan"]["alternate"],
                        i["flight_plan"]["aircraft_short"],
                        datetime.datetime.strptime(
                            i["logon_time"], "%Y-%m-%dT%H:%M:%S"
                        ).timestamp(),
                        datetime.datetime.strptime(
                            i["last_updated"], "%Y-%m-%dT%H:%M:%S"
                        ).timestamp(),
                        i["record_status"] == "normal",
                        i["airborne_time"],
                        i["flight_plan"]["route"],
                        fr,
                        i["flight_plan"]["flight_rules"] == "I",
                    ),
                )
                pilots_dumped += 1
            else:
                dbs[day][1].execute(
                    """INSERT INTO controllers (callsign, facility, rating, record_normal, logon_time, last_updated) VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        i["callsign"],
                        i["facility"],
                        i["rating"],
                        i["record_status"] == "normal",
                        datetime.datetime.strptime(
                            i["logon_time"], "%Y-%m-%dT%H:%M:%S"
                        ).timestamp(),
                        datetime.datetime.strptime(
                            i["last_updated"], "%Y-%m-%dT%H:%M:%S"
                        ).timestamp(),
                    ),
                )
                controllers_dumped += 1
        for i in dbs:
            dbs[i][0].commit()
            dbs[i][0].close()
        self.disc_conns = []
        self.log(
            f"Dumped data: {pilots_dumped} pilots, {controllers_dumped} controllers"
        )


vs = VatsimScraper(
    save_directory=config["save_directory"],
    log_directory=config["log_directory"],
    webhook_url=config["webhook_url"],
)

if __name__ == "__main__":
    try:
        vs.run()
    except KeyboardInterrupt:
        vs.stop()
