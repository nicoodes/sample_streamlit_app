import pandas as pd
import logging
import requests

import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = f"https://api.api-tennis.com/tennis/?method="


def get_fixtures(
    date_start: str,
    date_stop: str,
    player_key: str = "",
    base_url: str = BASE_URL,
    method: str = "get_fixtures",
    api_key: str = API_KEY,
) -> None:
    """Function to retrieve fixtures from the tenis API."""

    search = f"&date_start={date_start}&date_stop={date_stop}"

    if player_key != "":
        search += f"&player_key={player_key}"

    authentication = f"&APIkey={api_key}"
    url = base_url + method + search + authentication

    response = requests.get(url)
    if response.status_code == 500:
        logging.error("Server error (500)")
        logging.error(response.text)  # See the full error
        return None

    if response.status_code != 200:
        logging.error(f"HTTP Error {response.status_code}")
        logging.error(response.text)
        return None

    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError:
        logging.error("Response is not valid JSON despite Content-Type header")
        logging.error(response.text[:1000])
        return None


    if not data.get("result"):
        logging.info("No live scores available at the moment.")
        return


    # Lists to collect data
    events_records = []

    for event in data["result"]:

        # Save main event data (excluding nested lists)

        event_record = {
            "results_for_player_key": player_key,
            "event_key": event.get("event_key"),
            "event_date": event.get("event_date"),
            "event_time": event.get("event_time"),
            "event_first_player": event.get("event_first_player"),
            "first_player_key": event.get("first_player_key"),
            "event_second_player": event.get("event_second_player"),
            "second_player_key": event.get("second_player_key"),
            "event_final_result": event.get("event_final_result"),
            "event_game_result": event.get("event_game_result"),
            "event_serve": event.get("event_serve"),
            "event_winner": event.get("event_winner"),
            "event_status": event.get("event_status"),
            "event_type_type": event.get("event_type_type"),
            "tournament_name": event.get("tournament_name"),
            "tournament_key": event.get("tournament_key"),
            "tournament_round": event.get("tournament_round"),
            "tournament_season": event.get("tournament_season"),
            "event_live": event.get("event_live"),
            "event_qualification": event.get("event_qualification"),
            "event_first_player_logo": event.get("event_first_player_logo"),
            "event_second_player_logo": event.get("event_second_player_logo"),
        }
        events_records.append(event_record)

    # Save main events data
    df_events = pd.DataFrame(events_records)

    return df_events

if __name__ == "__main__":
    # Example usage
    df_fixtures = get_fixtures(date_start="2026-01-18", date_stop="2026-01-20")
    if df_fixtures is not None:
        print(df_fixtures.head())