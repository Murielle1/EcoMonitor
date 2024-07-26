from dash import Dash, html, dcc, callback, Input, Output
import plotly.express as px
import pandas as pd
from pyspark.sql import SparkSession

# Créer la session Spark
spark = SparkSession.builder \
    .appName("Nom de l'app") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

# Charger les données
df = spark.read.csv("pa/file.csv", header=True, inferSchema=True)
df = df.toPandas()

# Conversion des colonnes AQI en numérique
aqi_columns = ["AQI Value", "CO AQI Value", "Ozone AQI Value", "NO2 AQI Value", "PM2.5 AQI Value"]
for col in aqi_columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Créer l'application Dash
app = Dash(__name__)

# Mise en page de l'application
app.layout = html.Div(children=[
    html.H1(children='Analyse de la Qualité de l\'Air'),

    html.Div(children='''
        Visualisation des données de qualité de l'air par pays et par ville.
    '''),

    dcc.Graph(
        id='map-aqi'
    ),

    dcc.Dropdown(
        id='metric-dropdown',
        options=[
            {'label': 'AQI Value', 'value': 'AQI Value'},
            {'label': 'CO AQI Value', 'value': 'CO AQI Value'},
            {'label': 'Ozone AQI Value', 'value': 'Ozone AQI Value'},
            {'label': 'NO2 AQI Value', 'value': 'NO2 AQI Value'},
            {'label': 'PM2.5 AQI Value', 'value': 'PM2.5 AQI Value'}
        ],
        value='AQI Value',
        clearable=False,
        style={'width': '80%'}
    )
])

# Callback pour mettre à jour la carte
@app.callback(
    Output('map-aqi', 'figure'),
    Input('metric-dropdown', 'value')
)
def update_map(selected_metric):
    fig = px.scatter_geo(df, locations="Country",
                         locationmode='country names',
                         hover_name="City",
                         size=selected_metric,
                         title=f'Distribution de {selected_metric} par Pays et Ville',
                         projection="natural earth",
                         template="plotly")
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)