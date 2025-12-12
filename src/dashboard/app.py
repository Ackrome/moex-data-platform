# File: src/dashboard/app.py
import logging
import requests
import pandas as pd
import json
import traceback
import textwrap
from dash import Dash, dcc, html, Input, Output, State, ALL, callback_context, no_update
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket # <--- ВЕРНУЛИ
import dash_ace
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# --- CONFIG ---
API_URL = "http://127.0.0.1:8000"
WS_URL = "ws://127.0.0.1:8000/ws/tasks" # URL для вебсокета

import werkzeug
werkzeug_log = logging.getLogger('werkzeug')
werkzeug_log.setLevel(logging.ERROR)
werkzeug_log.disabled = True

# --- PRESETS ---
PRESETS = {
    "simple": {
        "label": "Simple: Price Line & SMA",
        "code": textwrap.dedent("""
        def custom_plot(df):
            import plotly.graph_objects as go
            import plotly.express as px
            fig = px.line(df, x='ts', y=['close', 'sma_20'], 
                          title='Price vs SMA-20',
                          template='plotly_dark')
            return fig
        """).strip()
    },
    "feature_eng": {
        "label": "Advanced: Feature Engineering",
        "code": textwrap.dedent("""
        def custom_plot(df):
            import plotly.graph_objects as go
            import plotly.express as px
            df['volatility'] = df['high'] - df['low']
            df['price_change'] = df['close'].pct_change() * 100
            fig = px.histogram(
                df, x="price_change", nbins=50,
                title="Distribution of Daily Price Changes (%)",
                template="plotly_dark",
                color_discrete_sequence=['#ff9800']
            )
            return fig
        """).strip()
    }
}

app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)

# --- HELPER ---
def get_auth_header(token_store):
    if not token_store: return {}
    return {"Authorization": f"Bearer {token_store['access_token']}"}

def fetch_tickers():
    try:
        resp = requests.get(f"{API_URL}/tickers", timeout=1)
        return resp.json() if resp.status_code == 200 else []
    except:
        return []

# --- LAYOUTS ---
def login_layout():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("🔐 MOEX Analytics Platform", className="text-center fw-bold"),
                    dbc.CardBody([
                        dbc.Tabs([
                            dbc.Tab([
                                html.Div([
                                    dbc.Input(id="login-username", placeholder="Username", type="text", className="mb-3 mt-3"),
                                    dbc.Input(id="login-password", placeholder="Password", type="password", className="mb-3"),
                                    dbc.Button("Login", id="login-btn", color="primary", className="w-100"),
                                    html.Div(id="login-error", className="text-danger mt-3 text-center small")
                                ])
                            ], label="Login"),
                            dbc.Tab([
                                html.Div([
                                    dbc.Input(id="reg-username", placeholder="New Username", type="text", className="mb-3 mt-3"),
                                    dbc.Input(id="reg-password", placeholder="Password", type="password", className="mb-3"),
                                    dbc.Button("Create Account", id="reg-btn", color="success", className="w-100"),
                                    html.Div(id="reg-error", className="text-info mt-3 text-center small")
                                ])
                            ], label="Register"),
                        ])
                    ])
                ], className="shadow-lg border-secondary bg-dark")
            ], width=12, md=4)
        ], className="justify-content-center align-items-center vh-100")
    ], fluid=True)

def dashboard_layout(username, role):
    is_admin = (role == 'admin')
    
    if is_admin:
        etl_content = dbc.Card([
            dbc.CardHeader([
                dbc.Row([
                    dbc.Col("⚡ Data Ingestion Pipeline", width=8, className="pt-1 fw-bold"),
                    dbc.Col(
                        dbc.Button("Hide Details", id="collapse-btn", color="link", size="sm", className="text-white text-decoration-none"),
                        width=4, className="text-end"
                    )
                ])
            ], className="bg-transparent border-bottom border-secondary"),
            dbc.CardBody([
                dbc.InputGroup([
                    dbc.Input(id='new-ticker-input', placeholder="Enter Ticker (e.g. SBER)", type="text"),
                    dbc.Button("Queue Task", id='etl-btn', color="primary")
                ]),
                dbc.Button("♻️ Restore / Resync All", id='restore-btn', color="warning", outline=True, size="sm", className="mt-3 w-100"),
                
                # Сюда будут падать задачи
                dbc.Collapse(html.Div(id='task-queue-container', className="mt-3"), id="task-collapse", is_open=True)
            ])
        ], className="h-100 shadow-sm border-secondary")
    else:
        etl_content = dbc.Card([
             dbc.CardBody([
                 html.H5("🔒 Access Restricted", className="text-center text-muted mt-4"),
                 html.P("Data Ingestion is available for Administrators only.", className="text-center text-muted")
             ])
        ], className="h-100 border-secondary")

    return dbc.Container([
        # ВЕРНУЛИ WebSocket (только здесь, внутри dashboard)
        WebSocket(id="ws", url=WS_URL),
        
        dcc.Store(id='task-store', data=[]),
        dcc.Store(id='sandbox-data-store'), 
        dcc.Download(id="download-raw-csv"),
        dcc.Download(id="download-sandbox-csv"),

        dbc.Row([
            dbc.Col(html.H2("MOEX Enterprise Analytics Platform", className="text-light fw-bold"), width=8),
            dbc.Col(html.Div([
                html.Span(f"👤 {username} ", className="me-2 text-info fw-bold"),
                html.Span(f"[{role}]", className="me-3 text-muted small"),
                dbc.Button("Logout", id="logout-btn", color="danger", size="sm", outline=True)
            ], className="text-end mt-2"), width=4)
        ], className="my-4"),

        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("📉 Visualization Control", className="fw-bold"),
                    dbc.CardBody([
                        html.Label("Select Asset:", className="text-muted small"),
                        dcc.Dropdown(id='ticker-dropdown', placeholder="Choose ticker...", className="text-dark mb-2"),
                        dbc.Row([
                            dbc.Col(dbc.RadioItems(id="interval-selector", options=[{"label": "Daily", "value": "1d"}, {"label": "Minute", "value": "1m"}], value="1d", inline=True, className="text-light mt-1"), width=7),
                            dbc.Col(dbc.Button("Download Raw CSV", id="btn-download-raw", color="success", size="sm", outline=True, className="w-100"), width=5)
                        ]),
                        dbc.Button("🔄 Refresh List", id='refresh-btn', color="secondary", size="sm", className="w-100 mt-2")
                    ])
                ], className="h-100 border-secondary")
            ], width=12, md=4),
            dbc.Col(etl_content, width=12, md=8)
        ], className="mb-4 g-4"),

        # ... остальной код (графики, песочница) без изменений ...
        dbc.Row([dbc.Col([dbc.Card([dbc.CardBody(dcc.Loading(id="loading-main", type="circle", children=dcc.Graph(id='main-chart', style={'height': '60vh'})))], className="border-secondary bg-dark shadow")], width=12)], className="mb-5"),
        dbc.Row([dbc.Col(html.Hr(className="text-secondary"), width=12), dbc.Col(html.H3("🛠 Custom Analytics Sandbox", className="text-center text-info mb-4"), width=12)]),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        dbc.Row([
                            dbc.Col("Code Editor", width=4, className="pt-1 fw-bold"),
                            dbc.Col(dcc.Dropdown(
                                id='preset-dropdown', 
                                options=[{'label': v['label'], 'value': k} for k, v in PRESETS.items()], # <--- ДОБАВИЛИ ЭТУ СТРОКУ
                                placeholder="Load Example...", 
                                className="text-dark", 
                                style={'fontSize': '12px'}
                            ), width=4),
                            dbc.Col(dcc.Dropdown(id='my-charts-dropdown', placeholder="My Saved Charts...", className="text-dark", style={'fontSize': '12px'}), width=4)
                        ])
                    ], className="bg-secondary text-white"),
                    dbc.CardBody([
                        dash_ace.DashAceEditor(id='code-editor', value=PRESETS['simple']['code'], theme='monokai', mode='python', height='450px'),
                        dbc.InputGroup([
                            dbc.Input(id="save-chart-name", placeholder="Chart Name..."),
                            dbc.Button("💾 Save", id="save-chart-btn", color="success"),
                            dbc.Button("▶ Run Analysis", id='run-code-btn', color="primary")
                        ], className="mt-2")
                    ], className="p-0")
                ], className="h-100 border-secondary")
            ], width=12, lg=5, className="mb-3"),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        dbc.Row([
                            dbc.Col([html.Span("Result", className="fw-bold me-2"), html.Span(id="dataset-info", className="badge bg-info text-dark")], width=8, className="pt-1"),
                            dbc.Col(dbc.Button("💾 Download Result", id="btn-download-sandbox", color="light", size="sm", className="w-100"), width=4)
                        ])
                    ], className="bg-secondary text-white"),
                    dbc.CardBody([dcc.Loading(type="cube", children=[html.Div(id='code-error-output'), dcc.Graph(id='custom-chart', style={'height': '400px'}), html.Div(id='data-preview', className="mt-2 small text-muted font-monospace")])])
                ], className="h-100 border-secondary")
            ], width=12, lg=7, className="mb-3")
        ], className="mb-5 pb-5 g-4")
    ], fluid=True)

# --- APP ---
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dcc.Store(id='auth-token', storage_type='local'),
    html.Div(id='page-content')
])

# --- CALLBACKS ---

# 1. LOGIN / LOGOUT / REGISTER
@app.callback([Output('auth-token', 'data'), Output('login-error', 'children')], [Input('login-btn', 'n_clicks')], [State('login-username', 'value'), State('login-password', 'value')], prevent_initial_call=True)
def login(n, username, password):
    if not username or not password: return no_update, "Enter credentials"
    try:
        resp = requests.post(f"{API_URL}/token", data={"username": username, "password": password})
        if resp.status_code == 200: return resp.json(), ""
        else: return None, "Invalid username or password"
    except: return None, "Server error (Check API)"

@app.callback(Output('reg-error', 'children'), [Input('reg-btn', 'n_clicks')], [State('reg-username', 'value'), State('reg-password', 'value')], prevent_initial_call=True)
def register(n, username, password):
    if not username or not password: return "Fill all fields"
    try:
        resp = requests.post(f"{API_URL}/register", json={"username": username, "password": password})
        if resp.status_code == 201: return "✅ Success! Please switch to Login tab."
        elif resp.status_code == 400: return "⚠️ Username already taken"
        else: return "❌ Error occurred"
    except: return "Server error"

@app.callback(Output('auth-token', 'clear_data'), Input('logout-btn', 'n_clicks'), prevent_initial_call=True)
def logout_action(n):
    if n: return True
    return no_update

@app.callback([Output('page-content', 'children'), Output('url', 'pathname')], [Input('url', 'pathname'), Input('auth-token', 'data')])
def router(pathname, token_data):
    is_authenticated = token_data and 'access_token' in token_data
    if pathname == '/login':
        if is_authenticated: return dashboard_layout(token_data['username'], token_data['role']), '/'
        else: return login_layout(), no_update
    if is_authenticated: return dashboard_layout(token_data['username'], token_data['role']), no_update
    else: return login_layout(), '/login'

# 2. ETL CONTROLS & WEBSOCKET
@app.callback([Output("task-collapse", "is_open"), Output("collapse-btn", "children")], [Input("collapse-btn", "n_clicks")], [State("task-collapse", "is_open")], prevent_initial_call=True)
def toggle_collapse(n, is_open):
    if n: return not is_open, "Show Details" if is_open else "Hide Details"
    return is_open, "Hide Details"

@app.callback(Output('new-ticker-input', 'value'), Input('etl-btn', 'n_clicks'), [State('new-ticker-input', 'value'), State('auth-token', 'data')], prevent_initial_call=True)
def queue_task(n, t, token):
    if t and token: requests.post(f"{API_URL}/etl/run", json={"tickers": [t.upper().strip()]}, headers=get_auth_header(token))
    return ""

@app.callback(Output('new-ticker-input', 'placeholder'), Input('restore-btn', 'n_clicks'), State('auth-token', 'data'), prevent_initial_call=True)
def restore_db(n, token):
    if n and token: requests.post(f"{API_URL}/etl/resync", headers=get_auth_header(token))
    return no_update

@app.callback(Output('task-queue-container', 'className'), Input({'type': 'cancel-btn', 'index': ALL}, 'n_clicks'), State('auth-token', 'data'), prevent_initial_call=True)
def cancel_task(n, token):
    ctx = callback_context
    if ctx.triggered and ctx.triggered[0]['value'] and token:
        try: requests.post(f"{API_URL}/etl/cancel/{json.loads(ctx.triggered[0]['prop_id'].split('.')[0])['index']}", headers=get_auth_header(token))
        except: pass
    return no_update

# --- WEBSOCKET LISTENER ---
@app.callback(Output('task-queue-container', 'children'), Input('ws', 'message'))
def update_queue(msg):
    if not msg or not msg['data']: return no_update
    tasks = json.loads(msg['data'])
    children = []
    for task in sorted(tasks, key=lambda x: x.get('state') == 'PENDING', reverse=True):
        st, pr = task.get('state', 'PENDING'), task.get('progress', 0)
        color = "warning" if st == 'PENDING' else "info" if st in ['RUNNING', 'PROGRESS'] else "success" if st == 'SUCCESS' else "danger"
        animated = st in ['PENDING', 'RUNNING', 'PROGRESS']
        cancel_btn = dbc.Button("✖", id={'type': 'cancel-btn', 'index': task['id']}, color="link", n_clicks=0, className="text-danger p-0 ms-2 text-decoration-none fw-bold") if animated else None
        
        children.append(dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([html.Strong(task['ticker']), html.Span(f" ({st})", className="ms-2 small text-muted")], width=3),
                    dbc.Col(
                        dbc.Progress(value=pr, label=f"{pr}%", color=color, striped=animated, animated=animated, style={"height": "20px"}, className="w-100"), 
                        width=5, className="d-flex align-items-center"
                    ),
                    dbc.Col(html.Small(task.get('status', '')), width=3, className="text-end text-truncate"),
                    dbc.Col(cancel_btn, width=1, className="text-end")
                ], align="center", className="g-0")
            ])
        ], className="mb-2 bg-dark border-secondary p-0"))
        if len(children) >= 5: break
    return children

# 3. DATA & AUTO REFRESH
# Обновляем выпадающий список тикеров, когда приходит сообщение от WebSocket (задача завершилась)
@app.callback(Output('ticker-dropdown', 'options'), [Input('refresh-btn', 'n_clicks'), Input('ws', 'message'), Input('url', 'pathname')])
def update_dd(n, msg, p): return [{'label': t, 'value': t} for t in fetch_tickers()]

# Остальные колбэки (графики, песочница) без изменений
@app.callback([Output('interval-selector', 'options'), Output('interval-selector', 'value')], [Input('ticker-dropdown', 'value')], [State('interval-selector', 'value')])
def update_int(t, v):
    d = [{"label": "Daily", "value": "1d", "disabled": True}, {"label": "Minute", "value": "1m", "disabled": True}]
    if not t: return d, "1d"
    try:
        a = requests.get(f"{API_URL}/availability/{t}").json()
        opts = [{"label": "Daily", "value": "1d", "disabled": not a.get('1d')}, {"label": "Minute", "value": "1m", "disabled": not a.get('1m')}]
        val = v if a.get(v) else ('1d' if a.get('1d') else '1m')
        return opts, val
    except: return d, "1d"

@app.callback(Output('main-chart', 'figure'), [Input('ticker-dropdown', 'value'), Input('interval-selector', 'value')])
def main_chart(t, i):
    empty = go.Figure(layout=go.Layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', xaxis={'visible': False}, yaxis={'visible': False}))
    if not t: return empty
    try:
        d = requests.get(f"{API_URL}/metrics/{t}?interval={i}").json()
        df = pd.DataFrame(d)
        df['ts'] = pd.to_datetime(df['ts'])
        fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05, row_heights=[0.6, 0.2, 0.2])
        fig.add_trace(go.Candlestick(x=df['ts'], open=df['open'], high=df['high'], low=df['low'], close=df['close'], name='OHLC'), row=1, col=1)
        fig.add_trace(go.Scatter(x=df['ts'], y=df['sma_20'], line=dict(color='yellow'), name='SMA20'), row=1, col=1)
        fig.add_trace(go.Bar(x=df['ts'], y=df['volume'], marker_color='teal', name='Vol'), row=2, col=1)
        fig.add_trace(go.Scatter(x=df['ts'], y=df['rsi_14'], line=dict(color='purple'), name='RSI'), row=3, col=1)
        fig.update_layout(template="plotly_dark", xaxis_rangeslider_visible=False, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=40, r=40, t=10, b=10), height=600)
        return fig
    except: return empty

@app.callback(Output("download-raw-csv", "data"), Input("btn-download-raw", "n_clicks"), [State('ticker-dropdown', 'value'), State('interval-selector', 'value')], prevent_initial_call=True)
def download_raw_csv(n, ticker, interval):
    if not ticker: return no_update
    try:
        url = f"{API_URL}/metrics/{ticker}?interval={interval}"
        df = pd.DataFrame(requests.get(url).json())
        return dcc.send_data_frame(df.to_csv, f"{ticker}_{interval}_raw.csv", index=False)
    except: return no_update

@app.callback(Output('my-charts-dropdown', 'options'), [Input('url', 'pathname'), Input('save-chart-btn', 'n_clicks')], State('auth-token', 'data'))
def update_my_charts(p, n, token):
    if not token: return []
    try:
        resp = requests.get(f"{API_URL}/charts", headers=get_auth_header(token))
        if resp.status_code == 200: return [{'label': c['name'], 'value': c['code']} for c in resp.json()]
    except: pass
    return []

@app.callback(Output('code-editor', 'value', allow_duplicate=True), [Input('preset-dropdown', 'value'), Input('my-charts-dropdown', 'value')], prevent_initial_call=True)
def load_code(preset, chart_code):
    ctx = callback_context
    if not ctx.triggered: return no_update
    trigger_id = ctx.triggered[0]['prop_id']
    if 'preset' in trigger_id and preset: return PRESETS[preset]['code']
    if 'my-charts' in trigger_id and chart_code: return chart_code
    return no_update

@app.callback(Output('save-chart-btn', 'children'), Input('save-chart-btn', 'n_clicks'), [State('save-chart-name', 'value'), State('code-editor', 'value'), State('auth-token', 'data')], prevent_initial_call=True)
def save_chart_action(n, name, code, token):
    if not name or not token: return "💾 Save"
    try:
        requests.post(f"{API_URL}/charts", json={"name": name, "code": code}, headers=get_auth_header(token))
        return "✅ Saved!"
    except: return "❌ Error"

@app.callback([Output('custom-chart', 'figure'), Output('code-error-output', 'children'), Output('dataset-info', 'children'), Output('data-preview', 'children'), Output('sandbox-data-store', 'data')], [Input('run-code-btn', 'n_clicks')], [State('code-editor', 'value'), State('ticker-dropdown', 'value'), State('interval-selector', 'value')], prevent_initial_call=True)
def run_custom_code(n, code, ticker, interval):
    empty = go.Figure(layout=go.Layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'))
    if not ticker: return empty, dbc.Alert("⚠️ Please select a ticker first.", color="warning"), "No Data", "", None
    try:
        resp = requests.get(f"{API_URL}/metrics/{ticker}?interval={interval}")
        data = resp.json()
        if not data: return empty, dbc.Alert(f"❌ No data for {ticker}.", color="danger"), "0 rows", "", None
        df = pd.DataFrame(data)
        df['ts'] = pd.to_datetime(df['ts'])
        for c in ['open', 'close', 'high', 'low', 'volume', 'sma_20', 'rsi_14']: df[c] = pd.to_numeric(df[c], errors='coerce')
        local_env = {'df': df, 'pd': pd, 'go': go, 'px': px, 'make_subplots': make_subplots}
        exec(code, {}, local_env)
        if 'custom_plot' not in local_env: return empty, dbc.Alert("Error: Function 'custom_plot' missing.", color="danger"), "", "", None
        fig = local_env['custom_plot'](df)
        fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        info_str = f"{len(df)} rows | {len(df.columns)} cols"
        preview_table = dbc.Table.from_dataframe(df.head(3), striped=True, bordered=True, hover=True, size='sm', color='dark')
        json_data = df.to_json(date_format='iso', orient='split')
        return fig, "", info_str, preview_table, json_data
    except Exception: return empty, dbc.Alert(html.Pre(traceback.format_exc()), color="danger"), "Error", "", None

@app.callback(Output("download-sandbox-csv", "data"), Input("btn-download-sandbox", "n_clicks"), [State('sandbox-data-store', 'data'), State('ticker-dropdown', 'value')], prevent_initial_call=True)
def download_sandbox_csv(n, json_data, ticker):
    if not json_data: return no_update
    try:
        df = pd.read_json(json_data, orient='split')
        return dcc.send_data_frame(df.to_csv, f"{ticker}_analysis_result.csv", index=False)
    except: return no_update

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=True)