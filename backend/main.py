# backend/app.py
from flask import Flask , request
from flask_socketio import SocketIO, emit, join_room, leave_room
from flask_cors import CORS
import pandas as pd
import geopandas as gpd
import random
import string
from math import radians, cos, sin, asin, sqrt
import time
import threading
import folium
from folium.plugins import MarkerCluster
from folium import Map, Choropleth, GeoJson
from folium.features import GeoJsonTooltip, Element
from shapely.geometry import Point
import os
from google.cloud import storage 
import io

app = Flask(__name__)
CORS(app)  # allows React to fetch from different port
@app.after_request
def allow_iframe(response):
    response.headers["X-Frame-Options"] = "ALLOWALL"
    return response

#Global Vars to prevent errors when I build before calling load
df = None
shapes_gdf = None
df_gdf = None
df_with_shapes = None
precomputed_categories = {}
choropleth_maps = {}

#Changing method of storage completely to Google Cloud Storage cause csv's total more than 100mb
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "crime-dataset-bucket")
PARQUET_FILE_NAME = "crime_dataset.parquet"
GEOJSON_FILE_NAME = "nyc_nta_2020.geojson"


def load_parquet(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    parquet = blob.download_as_bytes()

    df = pd.read_parquet(io.BytesIO(parquet))
    return df

def load_geojson(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    geojson = blob.download_as_bytes()

    gdf = gpd.read_file(io.BytesIO(geojson))
    return gdf

#making method for this so tracking a df or shapes failure is easier
def making_heatmap():
    #check if data loaded 
    if df is None or shapes_gdf is None:
        print("Error: Data not present in either shapes or df")
        return False
    
    global df_gdf
    global df_with_shapes
    global precomputed_categories
    global choropleth_maps

    df_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["Longitude"], df["Latitude"]),
        crs="EPSG:4326"
    )

    # Spatial join once
    df_with_shapes = gpd.sjoin(df_gdf, shapes_gdf, how="inner", predicate="intersects")

    category_rules = {
        "VANDALISM": {
            "include": ["CRIM MISCHIEF", "TRESPASS", "GRAFF"],
            "exclude": ["ASSAULT", "HARASSMENT"]
        },
        "DRUGS": {
            "include": ["NARCO", "MARIJUANA"],
            "exclude": []
        },
        "HARASSMENT": {
            "include": ["HARASSMENT", "VIOL ORDER PROTECT", "DOMESTIC", "FAMILY"],
            "exclude": ["ASSAULT"]
        },
        "ASSAULT": {
            "include": ["ASSAULT"],
            "exclude": []
        },
        "VEHICLE THEFT": {
            "include": ["LARCENY", "VEHICLE"],
            "exclude": []
        },
        "THEFT": {
            "include": ["LARCENY"],
            "exclude": ["VEHICLE"]
        },
        "BURGLARY": {
            "include": ["BURGLARY"],
            "exclude": []
        },
        "ROBBERY": {
            "include": ["ROBBERY"],
            "exclude": []
        },
        "SHOOTINGS": {
            "include": ["SHOT SPOTTER", "SHOTS", "FIREARM"],
            "exclude": []
        }
    }

    precomputed_categories.clear()

    for cat, rules in category_rules.items():
        mask = df_with_shapes["TYP_DESC"].str.contains("|".join(rules["include"]), case=False, na=False)
        if rules["exclude"]:
            mask &= ~df_with_shapes["TYP_DESC"].str.contains("|".join(rules["exclude"]), case=False, na=False)
        precomputed_categories[cat] = df_with_shapes[mask]

    choropleth_maps.clear()

    for cat, subset in precomputed_categories.items():
        counts = subset.groupby("NTA2020").size().reset_index(name="count")
        shapes_with_counts = shapes_gdf.merge(counts, on="NTA2020", how="left").fillna(0)

        m = folium.Map(location=[40.7128, -74.0060], zoom_start=11, tiles="CartoDB dark_matter")
        folium.Choropleth(
            geo_data=shapes_with_counts,
            data=shapes_with_counts,
            columns=["NTA2020", "count"],
            key_on="feature.properties.NTA2020",
            fill_color="YlOrRd",
            fill_opacity=0.8,
            line_opacity=0.3,
            nan_fill_color="gray",
            legend_name=f"{cat} Incidents"
        ).add_to(m)

        GeoJson(
            shapes_with_counts,
            style_function=lambda feature: {
                "fillColor": "transparent",
                "color": "transparent",
                "weight": 0
            },
            tooltip=GeoJsonTooltip(
                fields=["NTAName", "count"],
                aliases=["Neighborhood:", "Incidents:"],
                localize=True,
                sticky=True,
            )
        ).add_to(m)

        map_title = f"{cat} in NYC"
        subtitle = "Neighborhood incident counts"
        title_html = f"""
            <div style="
                position: fixed;
                top: 10px;
                left: 50%;
                transform: translateX(-50%);
                z-index: 9999;
                background-color: rgba(0, 0, 0, 0.6);
                padding: 6px 10px;
                border-radius: 4px;
                color: white;
                font-size: 14px;
                text-align: center;
            ">
                <b>{map_title}</b><br>{subtitle}
            </div>
        """
        m.get_root().html.add_child(Element(title_html))

        choropleth_maps[cat] = m.get_root().render()

    #check for 9 successful maps
    print(f" Made {len(choropleth_maps)} choropleth maps")
    return True

@app.route("/load")
def load_data():
    global df
    global shapes_gdf

    #testing GCS loading 
    try:
        df = load_parquet(GCS_BUCKET_NAME, PARQUET_FILE_NAME)
        print(f"loaded {len(df)} rows from parquet")

        #we expect 2.69 ish mil
        #for geojson we know it can work local since its not too much memory but we can make it uniform
        try:
            shapes_gdf = load_geojson(GCS_BUCKET_NAME, GEOJSON_FILE_NAME)
            print(f"success loading geojson from bucket")
        except Exception as e:
            geojson_path = os.path.join(os.path.dirname(__file__), "nyc_nta_2020.geojson")
            if os.path.exists(geojson_path):
                shapes_gdf = gpd.read_file(geojson_path)
            else:
                return f"couldn't find geojson on local or cloud look at pathing"

        if making_heatmap():
            return f"Created {len(choropleth_maps)} maps"
        else:
            return "Data loaded but process failed"

    except Exception as e:
        print(f"Error loading data boy")
        import traceback
        traceback.print_exc()
        return False

@app.route("/ping")
def ping():
    return "Backend is alive"

@app.route("/")
def default_map():
    m = Map(location=(40.7128, -74.0060), zoom_start=10, tiles="CartoDB dark_matter")
    return m._repr_html_()

@app.route("/maps/heatmap")
def crime_heatmap():
    # Get query parameter ?category=ASSAULT
    category = request.args.get("category", "ASSAULT").upper()

    if not choropleth_maps:
        if df is None:
            return "Data is still loading. Please wait...", 503
        return f"Data not loaded. Use /load", 503

    if category not in choropleth_maps:
        return f"Invalid category: {category}", 400
    
    return choropleth_maps[category]

def initialize_data():
    global df
    global shapes_gdf

    try:
        df = load_parquet(GCS_BUCKET_NAME, PARQUET_FILE_NAME)
        print(f"loaded {len(df)} rows from parquet")

        #we expect 2.69 ish mil
        #for geojson we know it can work local since its not too much memory but we can make it uniform
        try:
            shapes_gdf = load_geojson(GCS_BUCKET_NAME, GEOJSON_FILE_NAME)
            print(f"success loading geojson from bucket")
        except Exception as e:
            geojson_path = os.path.join(os.path.dirname(__file__), "nyc_nta_2020.geojson")
            if os.path.exists(geojson_path):
                shapes_gdf = gpd.read_file(geojson_path)
            else:
                return 

        if shapes_gdf is not None:
            if making_heatmap():
                print(f"Created {len(choropleth_maps)} maps")
            else:
                print("Data loaded but process failed")
        else:
            print("geojson failed to load")

    except Exception as e:
        print(f"Error loading data boy")
        import traceback
        traceback.print_exc()

_data_loading_started = False
_data_loading_complete = False

def load_data_background():
    global _data_loading_complete
    print("=" * 60)
    print("Starting background data loading...")
    print("=" * 60)
    try:
        initialize_data()
        _data_loading_complete = True
        print("=" * 60)
        print("Background data loading complete!")
        print(f"Loaded {len(df) if df is not None else 0} rows")
        print(f"Created {len(choropleth_maps)} choropleth maps")
        print("=" * 60)
    except Exception as e:
        print(f"ERROR in background data loading: {e}")
        import traceback
        traceback.print_exc()

if not _data_loading_started:
    _data_loading_started = True
    data_loading_thread = threading.Thread(target=load_data_background, daemon=True)
    data_loading_thread.start()
    print("Background data loading thread started")    

#GEOGUESSER CODE START

app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-me-in-production")
CORS(app, resources={r"/*": {"origins": "https://frontend-service-353447914077.us-east4.run.app"}})

# SocketIO setup
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="gevent",
    ping_timeout=60,
    ping_interval=25,
)

# Game state
games = {}

# Config
GOOGLE_MAPS_API_KEY = "AIzaSyDOuKyfb-Y2fJEfLOgfR46SVwkUn9NNoCE"
MAX_ROUNDS = 3

shooting_keywords = ["SHOT SPOTTER", "SHOTS", "FIREARM"]
robbery_keywords = ["ROBBERY"]
burglary_keywords = ["BURGLARY"]
harassment_keywords = ["HARASSMENT", "VIOL ORDER PROTECT", "DOMESTIC", "FAMILY"]
drug_keywords = ["NARCO", "MARIJUANA"]
vandalism_keywords = ["CRIM MISCHIEF", "TRESPASS", "GRAFF"]

# Crime colors for frontend chart
CRIME_COLORS = {
    "Shooting": "#EF553B",
    "Robbery": "#636EFA",
    "Burglary": "#AB63FA",
    "Theft (non vehicle)": "#FECB52",
    "Vehicle theft": "#FFA15A",
    "Assault": "#00CC96",
    "Harassment": "#19D3F3",
    "Drug": "#FF6692",
    "Vandalism": "#B6E880",
}


# Utility functions
def generate_room_code():
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


def haversine_distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 6371 * 2 * asin(sqrt(a))


def calculate_score(distance_km):
    if distance_km > 50:
        return 0
    return int(1000 * (1 - distance_km / 50))
    # if distance_km < 0.1:
    #     return 5000
    # elif distance_km < 1:
    #     return int(5000 * (1 - distance_km))
    # elif distance_km < 10:
    #     return int(3000 * (1 - distance_km / 10))
    # elif distance_km < 50:
    #     return int(1000 * (1 - distance_km / 50))
    # else:
    #     return 0


def get_zip_crime_counts(zip_code):
    """
    Count crimes by type for a given ZIP code.
    Returns a list of dictionaries for the bar chart.
    """
    if df is None or df.empty:
        return []

    # Filter to this ZIP code
    sub = df[df["ZIPCODE"] == zip_code]

    if sub.empty:
        print(f" No crimes found for ZIP {zip_code}")
        return []

    shooting = (
        sub["TYP_DESC"]
        .str.contains("|".join(shooting_keywords), case=False, na=False)
        .sum()
    )
    robbery = (
        sub["TYP_DESC"]
        .str.contains("|".join(robbery_keywords), case=False, na=False)
        .sum()
    )
    burglary = (
        sub["TYP_DESC"]
        .str.contains("|".join(burglary_keywords), case=False, na=False)
        .sum()
    )

    theft_non_vehicle = (
        sub["TYP_DESC"].str.contains("LARCENY", case=False, na=False)
        & ~sub["TYP_DESC"].str.contains("VEHICLE", case=False, na=False)
    ).sum()

    vehicle_theft = (
        sub["TYP_DESC"].str.contains("LARCENY", case=False, na=False)
        & sub["TYP_DESC"].str.contains("VEHICLE", case=False, na=False)
    ).sum()

    assault = sub["TYP_DESC"].str.contains("ASSAULT", case=False, na=False).sum()

    harassment = (
        sub["TYP_DESC"].str.contains(
            "|".join(harassment_keywords), case=False, na=False
        )
        & ~sub["TYP_DESC"].str.contains("ASSAULT", case=False, na=False)
    ).sum()

    drug = (
        sub["TYP_DESC"]
        .str.contains("|".join(drug_keywords), case=False, na=False)
        .sum()
    )

    vandalism = (
        sub["TYP_DESC"].str.contains("|".join(vandalism_keywords), case=False, na=False)
        & ~sub["TYP_DESC"].str.contains("ASSAULT", case=False, na=False)
        & ~sub["TYP_DESC"].str.contains("HARASSMENT", case=False, na=False)
    ).sum()

    # Build the data structure for frontend
    crime_data = [
        {
            "crime_type": "Shooting",
            "count": int(shooting),
            "color": CRIME_COLORS["Shooting"],
        },
        {
            "crime_type": "Robbery",
            "count": int(robbery),
            "color": CRIME_COLORS["Robbery"],
        },
        {
            "crime_type": "Burglary",
            "count": int(burglary),
            "color": CRIME_COLORS["Burglary"],
        },
        {
            "crime_type": "Theft (non vehicle)",
            "count": int(theft_non_vehicle),
            "color": CRIME_COLORS["Theft (non vehicle)"],
        },
        {
            "crime_type": "Vehicle theft",
            "count": int(vehicle_theft),
            "color": CRIME_COLORS["Vehicle theft"],
        },
        {
            "crime_type": "Assault",
            "count": int(assault),
            "color": CRIME_COLORS["Assault"],
        },
        {
            "crime_type": "Harassment",
            "count": int(harassment),
            "color": CRIME_COLORS["Harassment"],
        },
        {"crime_type": "Drug", "count": int(drug), "color": CRIME_COLORS["Drug"]},
        {
            "crime_type": "Vandalism",
            "count": int(vandalism),
            "color": CRIME_COLORS["Vandalism"],
        },
    ]

    # Filter out zero counts
    crime_data = [c for c in crime_data if c["count"] > 0]

    print(f"ZIP {zip_code} has {len(crime_data)} crime types with data")
    return crime_data


def get_random_location():
    """Get a random crime location with ZIP code crime statistics"""
    if df is None or df.empty:
        return None

    row = df.sample(n=1).iloc[0]

    # Get ZIP code and crime stats
    zip_code = row["ZIPCODE"]
    crime_stats = get_zip_crime_counts(zip_code)

    # Generate Street View URL if API key exists
    if GOOGLE_MAPS_API_KEY:
        street_view_url = f"https://maps.googleapis.com/maps/api/streetview?size=600x400&location={row['Latitude']},{row['Longitude']}&key={GOOGLE_MAPS_API_KEY}"
    else:
        street_view_url = ""

    return {
        "latitude": float(row["Latitude"]),
        "longitude": float(row["Longitude"]),
        "street_view_url": street_view_url,
        "zip_code": str(zip_code),
        "crime_stats": crime_stats,  # NEW: crime data for the chart
    }


# Start a round
def start_round(room_code):
    if room_code not in games:
        print(f" Cannot start round - room {room_code} does not exist")
        return

    game = games[room_code]

    if game["current_round"] >= MAX_ROUNDS:
        # End game
        final_scores = [
            {"player_name": p["name"], "score": p["score"]}
            for p in game["players"].values()
        ]
        final_scores.sort(key=lambda x: x["score"], reverse=True)

        print(
            f"🏆 Game ended in room {room_code}. Winner: {final_scores[0]['player_name']}"
        )

        socketio.emit(
            "game_end",
            {"final_scores": final_scores, "winner": final_scores[0]["player_name"]},
            room=room_code,
        )
        game["status"] = "game_end"
        return

    game["current_round"] += 1
    location = get_random_location()

    if location is None:
        print(f" Failed to get location for room {room_code}")
        socketio.emit("error", {"message": "Failed to get location"}, room=room_code)
        return

    game["current_location"] = location
    game["round_start_time"] = time.time()
    game["status"] = "playing"

    # Reset guesses
    for p in game["players"].values():
        p["guess"] = None

    print(
        f"✓ Round {game['current_round']} started in room {room_code} (ZIP: {location['zip_code']})"
    )

    socketio.emit(
        "round_start",
        {
            "round": game["current_round"],
            "total_rounds": MAX_ROUNDS,
            "location": {
                "street_view_url": location["street_view_url"],
                "zip_code": location["zip_code"],
                "crime_stats": location[
                    "crime_stats"
                ],  # NEW: send crime data to frontend
            },
            "time_limit": 30,
        },
        room=room_code,
    )


# Socket handlers
@socketio.on("connect")
def handle_connect():
    print(f" Client connected: {request.sid}")
    emit("connected", {"data": "Connected"})


@socketio.on("disconnect")
def handle_disconnect():
    sid = request.sid
    print(f" Client disconnected: {sid}")

    for room_code, game in list(games.items()):
        if sid in game["players"]:
            player_ids = list(game["players"].keys())
            is_host = player_ids[0] == sid if player_ids else False
            del game["players"][sid]

            if len(game["players"]) == 0:
                del games[room_code]
                print(f" Deleted empty room: {room_code}")
            elif is_host:
                print(f" Host left room {room_code} - closing room")
                emit(
                    "room_closed",
                    {"message": "Host left the game. Room has been closed."},
                    room=room_code,
                )
                del games[room_code]
            else:
                print(
                    f" Player left room {room_code} - {len(game['players'])} player(s) remaining"
                )
                emit(
                    "player_left",
                    {
                        "message": "Other player left the game",
                        "players": game["players"],
                    },
                    room=room_code,
                )


@socketio.on("create_room")
def handle_create_room(data):
    if df is None:
        emit("error", {"message": "Server error: Crime data not loaded"})
        return

    room_code = generate_room_code()
    player_name = data.get("player_name", "Player 1")
    player_id = request.sid

    games[room_code] = {
        "players": {player_id: {"name": player_name, "score": 0, "guess": None}},
        "current_round": 0,
        "total_rounds": MAX_ROUNDS,
        "current_location": None,
        "status": "waiting",
    }

    join_room(room_code)
    print(f" Room created: {room_code} by {player_name}")

    emit("room_created", {"room_code": room_code, "player_id": player_id})
    emit("player_joined", {"players": games[room_code]["players"]}, room=room_code)


@socketio.on("join_room")
def handle_join_room(data):
    room_code = data.get("room_code", "").upper()
    player_name = data.get("player_name", "Player 2")
    player_id = request.sid

    if room_code not in games:
        emit("error", {"message": "Room not found"})
        return

    if len(games[room_code]["players"]) >= 2:
        emit("error", {"message": "Room full"})
        return

    games[room_code]["players"][player_id] = {
        "name": player_name,
        "score": 0,
        "guess": None,
    }

    join_room(room_code)
    print(f" Player joined room {room_code}: {player_name}")

    emit("room_joined", {"room_code": room_code, "player_id": player_id})
    emit("player_joined", {"players": games[room_code]["players"]}, room=room_code)

    if len(games[room_code]["players"]) == 2:
        emit("ready_to_start", {}, room=room_code)


@socketio.on("start_game")
def handle_start_game(data):
    room_code = data.get("room_code")

    if room_code not in games:
        emit("error", {"message": "Room not found"})
        return

    game = games[room_code]
    if len(game["players"]) < 2:
        emit("error", {"message": "Need 2 players to start"})
        return

    print(f" Starting game in room {room_code}")
    start_round(room_code)


@socketio.on("submit_guess")
def handle_submit_guess(data):
    room_code = data.get("room_code")
    player_id = request.sid
    guess_lat = data.get("latitude")
    guess_lng = data.get("longitude")

    if room_code not in games or player_id not in games[room_code]["players"]:
        emit("error", {"message": "Invalid game or player"})
        return

    game = games[room_code]
    game["players"][player_id]["guess"] = {
        "latitude": guess_lat,
        "longitude": guess_lng,
    }

    print(
        f" Guess submitted in room {room_code} by {game['players'][player_id]['name']}"
    )

    if all(p["guess"] is not None for p in game["players"].values()):
        actual = game["current_location"]
        round_results = []

        for pid, player in game["players"].items():
            dist = haversine_distance(
                actual["latitude"],
                actual["longitude"],
                player["guess"]["latitude"],
                player["guess"]["longitude"],
            )
            score = calculate_score(dist)
            player["score"] += score
            round_results.append(
                {
                    "player_id": pid,
                    "player_name": player["name"],
                    "distance_km": round(dist, 2),
                    "round_score": score,
                    "total_score": player["score"],
                    "guess": player["guess"],
                }
            )

        print(f" Round {game['current_round']} completed in room {room_code}")

        emit(
            "round_end",
            {
                "actual_location": actual,
                "results": round_results,
                "current_round": game["current_round"],
            },
            room=room_code,
        )
        game["status"] = "round_end"


@socketio.on("ready_for_next_round")
def handle_ready_for_next_round(data):
    room_code = data.get("room_code")
    player_id = request.sid

    if room_code not in games or player_id not in games[room_code]["players"]:
        emit("error", {"message": "Invalid game or player"})
        return

    game = games[room_code]
    player_name = game["players"][player_id]["name"]

    print(f" Player {player_name} clicked next round - advancing room {room_code}")

    start_round(room_code)

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("NYC Crime GeoGuessr Server")
    print("=" * 60)
    import time
    time.sleep(2) 

    if df is None:
        data_loading_thread.join(timeout=120)
        if df is None:
            print("Background loading timed out. Loading synchronously...")
            initialize_data()
    
    if df is not None:
        print(f" Crime data: {len(df)} records loaded")
        print(f" Created {len(choropleth_maps)} choropleth maps")
    else:
        print(" Crime data didn't load")
    print(f" Server starting on http://localhost:8080")
    print("=" * 60 + "\n")
    
    socketio.run(app, host="0.0.0.0", port=8080, debug=True, allow_unsafe_werkzeug=True)
