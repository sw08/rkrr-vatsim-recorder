import time, datetime, requests, json, os, logging
from discord_webhook import DiscordWebhook


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
        if not os.path.exists(os.path.join(self.save_directory, "pilots")):
            os.mkdir(os.path.join(self.save_directory, "pilots"))
        if not os.path.exists(os.path.join(self.save_directory, "controllers")):
            os.mkdir(os.path.join(self.save_directory, "controllers"))
        if not os.path.exists(self.log_directory):
            os.mkdir(self.log_directory)
        self.active = True
        self.pilots = {}
        self.controllers = {}
        self.webhook_url = webhook_url
        self.cdata = []
        self.pdata = []
        self.update()

    @staticmethod
    def is_same_connection(i1, i2):
        if not (i1["cid"] == i2["cid"] and i1["callsign"] == i2["callsign"]):
            return False

        fp1 = i1["flight_plan"] if "flight_plan" in i1 else None
        fp2 = i2["flight_plan"] if "flight_plan" in i2 else None
        if (a := (fp1 is None)) ^ (b := (fp2 is None)):
            return False
        if not a and not b:
            return fp1["revision_id"] == fp2["revision_id"]

    def new_connection(self, conn_type, data):
        if conn_type == "pilot":
            self.pilots[data["callsign"]] = data
            self.pilots[data["callsign"]]["end_status"] = "normal"
        else:
            self.controllers[data["callsign"]] = data
            self.controllers[data["callsign"]]["end_status"] = "normal"
        self.log(f"New {conn_type} connection: {data['callsign']}")

    def end_connection(self, conn_type, callsign):
        if conn_type == "pilot":
            self.pdata.append(self.pilots[callsign])
            del self.pilots[callsign]
        else:
            self.cdata.append(self.controllers[callsign])
            del self.controllers[callsign]
        self.log(f"Ended {conn_type} connection: {callsign}")

    def update_last_seen(self, conn_type, data):
        if conn_type == "pilot":
            self.pilots[data["callsign"]]["last_updated"] = data["last_updated"]
        else:
            self.controllers[data["callsign"]]["last_updated"] = data["last_updated"]

    # def filter_rkrr(self, connections):
    #     result = []
    #     for conn in connections:

    def update(self):
        try:
            response = requests.request("GET", url, headers=headers).json()
            current_pilots = response["pilots"]
            pilot_updated = 0
            for p in current_pilots:
                if p is None:
                    raise Exception("Received NoneType pilot data from VATSIM.")
                if p["callsign"] not in self.pilots:
                    self.new_connection("pilot", p)
                elif not VatsimScraper.is_same_connection(
                    p, self.pilots[p["callsign"]]
                ):
                    self.end_connection("pilot", p["callsign"])
                    self.new_connection("pilot", p)
                else:
                    self.update_last_seen("pilot", p)
                pilot_updated += 1
            for p in self.pilots:
                if p not in [x["callsign"] for x in current_pilots]:
                    self.end_connection("pilot", p["callsign"])
                    pilot_updated += 1

            current_controllers = response["controllers"]
            controller_updated = 0
            for c in current_controllers:
                if c["callsign"] not in self.controllers:
                    self.new_connection("controller", c)
                elif not VatsimScraper.is_same_connection(
                    c, self.controllers[c["callsign"]]
                ):
                    self.end_connection("controller", c["callsign"])
                    self.new_connection("controller", c)
                else:
                    self.update_last_seen("controller", c)
                controller_updated += 1
            for c in self.controllers:
                if c not in [x["callsign"] for x in current_controllers]:
                    self.end_connection("controller", c["callsign"])
                    controller_updated += 1
            return {
                "ok": True,
                "data": f"Pilots updated: {pilot_updated}, Controllers updated: {controller_updated}",
            }
        except Exception as e:
            # raise e  # disable error handling for test
            DiscordWebhook(
                url=self.webhook_url,
                content=f"VATSIM Scraper Error: {str(e)}",
            ).execute()
            return {"ok": False, "error": str(e)}

    def run(self):
        day = datetime.datetime.now().day
        while self.active:
            if day != (new := datetime.datetime.now().day):
                day = new
                self.dump_data()
                self.log(f"New day: data reset at {day}")
            result = self.update()
            if result["ok"]:
                self.log(f"Update successful: {result['data']}")
            else:
                self.log(f"Update failed: {result['error']}")
            time.sleep(300)

    def stop(self):
        for i in list(self.controllers.keys()):
            self.controllers[i]["end_status"] = "scraper_stopped"
            self.end_connection("controller", i)
        for i in list(self.pilots.keys()):
            self.pilots[i]["end_status"] = "scraper_stopped"
            self.end_connection("pilot", i)
        self.active = False
        self.dump_data()
        self.log(f"Scraper stopped at {datetime.datetime.now()}")

    def log(self, message):
        today = datetime.datetime.now().strftime("%y%m%d")
        with open(os.path.join(self.log_directory, today + ".json"), "a") as log_file:
            log_file.write(f"{datetime.datetime.now()}: {message}\n")
        print(f"{datetime.datetime.now()}: {message}")

    def dump_data(self):
        today = datetime.datetime.now().strftime("%y%m%d")
        pdata = self.pdata
        if os.path.isfile(
            route := os.path.join(self.save_directory, "pilots", today + ".json")
        ):
            with open(route, "r") as f:
                pdata = json.load(f)
            pdata.extend(self.pdata)
            self.log(f"Existing pilot data found for {today}, appending data.")
        with open(
            os.path.join(self.save_directory, "pilots", today + ".json"), "w"
        ) as f:
            json.dump(pdata, f)
        self.log(f"Pilot data dumped for {today}: {len(self.pdata)} records.")
        self.pdata = []

        cdata = self.cdata
        if os.path.isfile(
            route := os.path.join(self.save_directory, "controllers", today + ".json")
        ):
            with open(route, "r") as f:
                cdata = json.load(f)
            cdata.extend(self.cdata)
            self.log(f"Existing controller data found for {today}, appending data.")
        with open(
            os.path.join(self.save_directory, "controllers", today + ".json"), "w"
        ) as f:
            json.dump(cdata, f)
        self.log(f"Controller data dumped for {today}: {len(self.cdata)} records.")
        self.cdata = []


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
