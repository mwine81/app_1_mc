# %%
import polars as pl
from polars import col as c
import polars.selectors as cs
import plotly.express as px
from pathlib import Path
MCCPDC_PRIMARY = '#12366c'
MCCPDC_SECONDARY = '#dcf2f9'
MCCPDC_ACCENT = '#f06842'
DATA = Path("data/*.parquet")
#NADAC_FEE = 12
diff = (c.total - c.mc_total).alias("diff")
TOP_SAVINGS_DICT = {'Total Spend':'total','Total Savings':'diff','Avg Savings Per Rx':'per_rx'}
GROUP_DICT ={
    "Antihistamines/Nasal Agents/Cough & Cold/Respiratory/Misc (41-45)":'Cold/Cough/Allergy',
    "Neuromuscular Agents (72-76)" :'Neuromuscular',
    "Gastrointestinal Agents (46-52)" :'Stomach/GI',
    "Anti-Infective Agents (01-16)": 'Anti-Infective',
    "Endocrine and Metabolic Agents (22-30)": 'Thyroid/Metabolic',
    "ADHD/Anti-Narcolepsy /Anti-Obesity/Anorexiant Agents (61-61)": 'ADHD/Obesity/Anorexiant',
    "Nutritional Products (77-81)": 'Nutritional',
    "Central Nervous System Agents (57-60)": 'Central Nervous System',
    "Genitourinary Antispasmodics/Vaginal Products/Misc (53-56)": 'Genitourinary',
    "Hematological Agents (82-85)": 'Blood/Hematological',
    "Psychotherapeutic and Neurological Agents - Miscellaneous (62-63)": 'Parkinson/Neurological',
    "Miscellaneous Products (92-99)": 'Miscellaneous',
    "Dermatological/Anorectal/Mouth-Throat/Dental/Ophthalmic/Otic (86-91)": 'Dermatological/ENT',
    "Analgesic/Anti-Inflammatory/Migraine/Gout Agents/Anesthetics (64-71)": 'Analgesic/Anesthetics',
    "Antineoplastic Agents and Adjunctive Therapies (21-21)": 'Cancer',
    "Cardiovascular Agents (31-40)": 'Cardiovascular',
}

def get_data_sets():
    return [file.stem for file in Path('data').iterdir()]


def load_data(files):
    paths = files if not None else 'data/*.parquet'
    return (
        pl.scan_parquet(paths)
        .with_columns(diff)
        .with_columns(c.drug_class.replace(GROUP_DICT).alias('drug_class'))
        )

def generate_label(value, prefix=None):
    return (
        pl.when(value >= 1_000_000)
        .then(pl.format("${}M", (value / 1_000_000).round(2).cast(pl.String)))
        .when(value >= 1_000)
        .then(pl.format("${}K", (value / 1_000).round(2).cast(pl.String)))
        .otherwise(pl.format("${}", value.round(2).cast(pl.String)))
        .name.suffix("_label")
    )


def ban_frame(data):
    data = (
        data
        .drop("year", "month")
        .select(cs.numeric().sum())
        .with_columns(saving_per_rx())
        .with_columns(
            generate_label(c.total), generate_label(c.mc_total), generate_label(c.diff)
        )
        .collect()
    )
    total = data.select(c.total_label).item()
    mc_total = data.select(c.mc_total_label).item()
    diff = data.select(c.diff_label).item()
    rx_ct = data.select(c.rx_ct).item()
    per_rx = data.select(c.per_rx).item()
    return {
        "total": total,
        "mc_total": mc_total,
        "diff": diff,
        "rx_ct": rx_ct,
        "per_rx": per_rx,
    }


def saving_per_rx():
    return (c.diff / c.rx_ct).round(2).alias("per_rx")

def top_saving_drugs(data):
    #rk = TOP_SAVINGS_DICT.get(rank_by)
    #sort_col = 'per_rx' if rk == 'per_rx' else 'diff'
    data = (
        data.group_by(c.generic_name)
        .agg(c.diff.sum(), c.total.sum(), c.mc_total.sum(),c.rx_ct.sum())
        .with_columns(saving_per_rx())
        .sort('diff', descending=True)
        .head(10)
        .sort(by='diff')
        .collect()
    )

    fig = px.bar(
        data,
        y="generic_name",
        x='diff',
        title=f"MCCPDC Savings - Top 10 Drugs by Total Savings($)",
        orientation="h",
        barmode="group",
        #text_auto=True,
        text=data['diff'],
        color_discrete_sequence=[MCCPDC_PRIMARY],
    )
    fig.update_traces(texttemplate="%{text:$,.0f}",)
    fig.update_layout(
        showlegend=False,
        height=50*10,
    )

    fig.update_xaxes(
        # tickformat="$,.0f",
        showticklabels=False,
        title = '',

    )
    fig.update_yaxes(
        title = '',
        ticksuffix='    '

    )

    return fig

def fig_drug_group(data,rank_by):
    rk = TOP_SAVINGS_DICT.get(rank_by)
    data = (
        data.group_by(c.drug_class, c.generic_name)
        .agg(c.diff.sum(),c.total.sum(),c.rx_ct.sum())
        .with_columns(saving_per_rx())
    )
    fig = px.pie(
        data.collect(),
        values=rk,
        names="drug_class",
        hole=.7,
    )
    # fig.update_layout(
    #     paper_bgcolor="rgba(0,0,0,0)",  # Transparent outer background
    #     plot_bgcolor="rgba(0,0,0,0)",)
    return fig


def nadac_plus(fee):
    return (c.nadac + (c.rx_ct * fee)).alias('nadac')


def fig_monthly_spend(data,nadac_fee):

    data = (
        data
        .filter(c.nadac.is_not_null())
        .with_columns(nadac_plus(nadac_fee))
        .group_by(pl.date(c.year, c.month, 1).alias('dos'))
        .agg(pl.col('total', 'mc_total', 'nadac').sum())
        .sort(c.dos)
    )
    fig = px.line(data.collect(),
                  x='dos',
                  y=['total', 'mc_total', 'nadac'],
                  line_shape='spline',
                  color_discrete_map={'mc_total':MCCPDC_PRIMARY,'nadac':MCCPDC_SECONDARY,'total':MCCPDC_ACCENT}
                  )
    fig.update_layout(
        plot_bgcolor = 'white',
        legend=dict(
        title='',
        orientation="h",  # Set legend orientation to horizontal
        x=.1,  # Set the x-position of the legend (centered)
        xanchor="center",  # Anchor the legend at the center
        y=1.2,  # Adjust the y-position (above the plot)
    )
                      )
    fig.update_traces(
        line=dict(width=4),
        opacity=0.60,
    )
    fig.update_xaxes(title='')
    fig.update_yaxes(title='Spend($)')



    return fig


def average_charge_per_rx_fig(data):
    data = (
        data
        .select(c.total.sum(), c.mc_total.sum(), c.rx_ct.sum())
        .select(pl.col('total', 'mc_total') / c.rx_ct)
        .unpivot()
    )
    fig = px.bar(
        data.collect(),
        x="variable",
        y="value",
        color="variable",
        text="value",
        color_discrete_map={'mc_total': MCCPDC_PRIMARY, 'total': MCCPDC_ACCENT}
    )
    fig.update_yaxes(
        showticklabels=False,
        title='',
        range=[0, data.select(c.value).max().collect().item() * 1.2]
    )
    # Style the labels (optional)
    fig.update_traces(
        texttemplate="%{text:$.2f}",  # Format the text labels
        textposition="outside",
        textfont_size=20,
        textfont_color=MCCPDC_PRIMARY,
        #textfont_color = 'white'
    )
    fig.update_xaxes(
        title='',
        showticklabels=False,
    )
    fig.update_layout(
        plot_bgcolor='white',
        legend=dict(
            orientation="h",  # Set legend orientation to horizontal
            x=.75,  # Set the x-position of the legend (centered)
            xanchor="center",  # Anchor the legend at the center
            y=1.2,  # Adjust the y-position (above the plot),
            title='',
        ),

    )
    return fig

