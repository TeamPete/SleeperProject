import requests
import re
import pandas as pd

# Create function that returns a python dictionary of nfl player info
def get_playerinfo():
    url = "https://api.sleeper.app/v1/players/nfl"

    try:
        response = requests.get(url)
        response.raise_for_status
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"There was an error when fetching roster data from your league: {e}")
        return {}

# Create function that filters player data    
def filter_playerinfo(data):
    players = []
    offense = ['QB', 'WR', 'TE', 'RB']

    # We only want to select the relevant fields
    for key, value in data.items():
        try:
            # Only filter for fantasy relevant players
            if value.get('position') in offense:
                players.append({
                    'player_id': int(key),
                    'name': value.get('full_name'),
                    'position': value.get('position'),
                    'team': value.get('team'),
                    'exp': value.get('years_exp'),
                    'age': value.get('age'),
                    'height': value.get('height'),
                    'weight': value.get('weight'),
                    'college': value.get('college'),
                    'status': value.get('status')
                })

        except (ValueError, TypeError) as e:
            # Handle conversion error or missing value
            print(f"Error processing player {key}: {e}")

    return pd.DataFrame(players)

def convert_to_inches(height):
    if "'" in height:
        # If height is in feet and inches format, create match object
        match_obj = re.match(r"(\d+)'(\d+)\"", height)

        if match_obj:
            feet = int(match_obj.group(1))
            inches = int(match_obj.group(2))
            return feet * 12 + inches
        else:
            raise ValueError("Invalid height format")
    else:
        return height
    
response_as_dict = get_playerinfo()
players_df = filter_playerinfo(response_as_dict)
players_df['height'] = players_df['height'].apply(convert_to_inches)

players_df.to_csv("nfl_players.csv", index=False)