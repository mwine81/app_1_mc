from dash import Dash, html, dcc, Output, Input, State
import dash_bootstrap_components as dbc
from logic import ban_frame, top_saving_drugs, fig_drug_group, fig_monthly_spend, get_data_sets, load_data, \
    TOP_SAVINGS_DICT, MCCPDC_PRIMARY, MCCPDC_ACCENT,average_charge_per_rx_fig
from polars import col as c
# Initialize the Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

def generate_kpi(kpi, description,style=None):
    return dbc.Col(dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H2(kpi, className="card-title text-center"),
                    html.P(description, className="card-text text-center"),
                ]
            ),
        ],
        className="bg-light",style=style))

def create_kpi(files):
    ban = ban_frame(files)
    kpis = [
        generate_kpi(ban.get('total'), 'Total Charge',style={'color':MCCPDC_PRIMARY,'border':f'2px solid {MCCPDC_PRIMARY}'}),
        generate_kpi(ban.get('mc_total'), 'MCCPDC Total',style={'color':MCCPDC_PRIMARY,'border':f'2px solid {MCCPDC_PRIMARY}'}),
        generate_kpi(ban.get('diff'), 'MCCPDC Savings',style={'color':MCCPDC_ACCENT,'border':f'2px solid {MCCPDC_ACCENT}'}),
        generate_kpi('{:,}'.format(ban.get('rx_ct')), 'Total Rx Count',style={'color':MCCPDC_PRIMARY,'border':f'2px solid {MCCPDC_PRIMARY}'}),
        generate_kpi('${:,}'.format(ban.get('per_rx')), 'Avg. Savings Per Rx',style={'color':MCCPDC_PRIMARY,'border':f'2px solid {MCCPDC_PRIMARY}'}),
    ]
    return kpis

def generate_drop_down(name,**kwargs):
    return dbc.Col(dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H6(name, className="card-title"),
                    dcc.Dropdown(**kwargs)
                ]
            ),
        ],
        className="bg-light",style={'color':MCCPDC_PRIMARY,'border':f'1px solid {MCCPDC_PRIMARY}'}))

CONTROLS = (
    #generate_drop_down('Data Sets', options =get_data_sets(), value= get_data_sets(),multi=True, id='data-sets'),
    generate_drop_down('Drug Class Filter:',multi=True, id='controls-drug_class'),
    #generate_drop_down('Product',multi=True, id='controls-product'),
    generate_drop_down('PBM Affiliated Pharmacy Filter:',options =['All', 'Affiliated', 'Non-Affiliated'], value='All',multi=False, id='controls-affiliated'),
    generate_drop_down('Claims To Include Filter:',options =['All Claims', 'MCCPDC Savings Only'], value='All',multi=False, id='controls-mc-less'),
)
CONTROLS_2 = (
    generate_drop_down('Data Sets to Utilize Filter:', options =get_data_sets(), value= get_data_sets(),multi=True, id='data-sets'),
    generate_drop_down('Product Filter:',multi=True, id='controls-product'),
    #generate_drop_down('MCCPDC Less',options =['All Claims', 'MCCPDC Savings Only'], value='All',multi=False, id='controls-mc-less'),
)


def get_files(data_sets):
    if data_sets:
        return [f'data/{file}.parquet' for file in data_sets]
    return [f'data/{file}.parquet' for file in get_data_sets()]

def data_for_fig(affiliated_filter, data_sets, drug_class, product, mc_less_filter):
    files = get_files(data_sets)
    data = load_data(files)
    if drug_class:
        data = data.filter(c.drug_class.is_in(drug_class))
    if product:
        data = data.filter(c.product.is_in(product))
    if affiliated_filter == 'Affiliated':
        data = data.filter(c.affiliated == True)
    if affiliated_filter == 'Non-Affiliated':
        data = data.filter(c.affiliated == False)
    if mc_less_filter == 'MCCPDC Savings Only':
        data = data.filter(c.is_less)
    return data



app.layout = (
    dbc.Container([
        dbc.Row(
            dbc.Col(
                html.H1('Mark Cuban Drug Company',),
                style={"backgroundColor":MCCPDC_PRIMARY},
                className="p-3 m-3 text-white rounded",
            ),
        )
        ,
        dbc.Row(justify="between", id='kpi-row'),
        dbc.Row(CONTROLS_2, class_name='mt-3'),
        dbc.Row(CONTROLS, class_name='mt-3'),
        dbc.Row(
            [dbc.Col(
                [
                    html.H1('Saving By Drug Class'),
                    dbc.Label('Rank By:'),
                    dcc.Dropdown(options=[x for x in TOP_SAVINGS_DICT],value='Total Savings', style={'width': '200px'},id='rank-by-pie',className='m-2'),
                    dcc.Graph(id='fig_drug_group')
                ],
                style={'color':MCCPDC_PRIMARY,'border':f'1px solid {MCCPDC_PRIMARY}'},
                className="bg-light p-3 m-3 rounded",
            ),
            dbc.Col([
                dbc.Row(dbc.Col([
                    dbc.Label('Top N results:',className="p-3"),
                    dcc.Input(id='n-results', value=10, type='number',min=1,max=20),
                    dbc.Label('Rank By:'),
                    dcc.Dropdown(options=[x for x in TOP_SAVINGS_DICT],value='Total Spend', style={'width': '200px'},id='rank-by')
                ],class_name='hstack gap-3 align-middle'),
                ),
                    dcc.Graph(id='fig_top_savings')
                ],
                style={'color':MCCPDC_PRIMARY,'border':f'1px solid {MCCPDC_PRIMARY}'},
                className="bg-light p-3 m-3 rounded"
            )],


        ),
            dbc.Row([
                dbc.Col([
                    html.H1('Average Charge Per Rx'),
                    dcc.Graph(id='avg-charge-rx')
                ],
                style={'color':MCCPDC_PRIMARY,'border':f'1px solid {MCCPDC_PRIMARY}'},
                className="bg-light p-3 m-3 rounded", width=4),
                dbc.Col(
                    [
                    html.H1('Spending by Month'),
                    dbc.Label('NADAC Fee Per Rx:'),
                    dcc.Input(id='nadac_fee', value=10, type='number',min=1,max=20,className='m-3'),
                    dcc.Graph(id='fig-monthly-spend')
            ],
            style={'color':MCCPDC_PRIMARY,'border':f'1px solid {MCCPDC_PRIMARY}'},
            className="bg-light p-3 m-3 rounded")]
            )


        ],className="m-1 p-2 rounded", style={"background-color": "snow",'border':f'1px solid {MCCPDC_PRIMARY}'},fluid=True)
)

@app.callback(
Output('kpi-row', 'children'),
    Output('fig_drug_group','figure'),
    Output('fig_top_savings','figure'),
    Output('fig-monthly-spend','figure'),
    Output('avg-charge-rx','figure'),
    Input('data-sets', 'value'),
    Input('controls-drug_class', 'value'),
    Input('controls-product', 'value'),
    Input('controls-affiliated', 'value'),
    Input('controls-mc-less', 'value'),
    Input('n-results', 'value'),
    Input('rank-by', 'value'),
    Input('nadac_fee', 'value'),
    Input('rank-by-pie', 'value')
)
def update_group_drug_fig(data_sets,drug_class, product, affiliated_filter,mc_less_filter,n_results,rank_by,nadac_fee,rank_by_pie_value):
    data = data_for_fig(affiliated_filter, data_sets, drug_class, product,mc_less_filter)
    return create_kpi(data),fig_drug_group(data,rank_by_pie_value),top_saving_drugs(data),fig_monthly_spend(data,nadac_fee),average_charge_per_rx_fig(data)

@app.callback(
Output('controls-drug_class','options'),
    Input('data-sets','value'),
    Input('controls-affiliated', 'value')
)
def update_drug_class(data_sets,affiliated_filter):
    files = get_files(data_sets)
    data = load_data(files)
    if affiliated_filter == 'Affiliated':
        data = data.filter(c.affiliated == True)
    if affiliated_filter == 'Non-Affiliated':
        data = data.filter(c.affiliated == False)
    data = (
        data
        .select(c.drug_class)
        .unique()
        .sort(by='drug_class')
        .collect()
        .to_series()
        .to_list()
    )
    return data

@app.callback(
Output('controls-product','options'),
    Input('data-sets','value'),
    Input('controls-drug_class','value'),
    Input('controls-affiliated', 'value')

)
def update_product(data_sets,drug_class_filter,affiliated_filter):
    files = get_files(data_sets)
    data = load_data(files)
    if affiliated_filter == 'Affiliated':
        data = data.filter(c.affiliated == True)
    if affiliated_filter == 'Non-Affiliated':
        data = data.filter(c.affiliated == False)
    if drug_class_filter:
        data = data.filter(c.drug_class.is_in(drug_class_filter))

    data = (
        data
        .select(c.product)
        .unique()
        .sort(by='product')
        .collect()
        .to_series()
        .to_list()
    )
    return data


if __name__ == '__main__':
    app.run(debug=True)