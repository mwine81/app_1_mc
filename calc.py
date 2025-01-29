import polars as pl
from polars import col as c
import polars.selectors as cs
import duckdb
from pyarrow import infer_type

m = pl.scan_parquet("/Users/matthewwine/3 Axis Advisors Dropbox/Matthew matt@3axisadvisors.com/datalake/assets/medispan.parquet")
#DATA_PATH = "file:///Users/matthewwine/3 Axis Advisors Dropbox/Matthew matt@3axisadvisors.com/datalake/projects/Jan_2025/mccpc_wv_oig/wv_mccpdc_claims.parquet"
# (
# pl.scan_parquet(DATA_PATH)
# .filter(c.is_mccpdc)
# .filter(c.is_brand == False)
# .join(m.select(c.ndc,c.gpi_10_generic_name,c.gpi_0_category.alias('drug_class')),on='ndc')
# .group_by(c.product,c.gpi_10_generic_name.alias('generic_name'),c.drug_class,c.dos.dt.year().alias('year'),c.dos.dt.month().alias('month'),(c.mc_total < c.total).alias('is_less'),c.affiliated)
# .agg(pl.col('total','mc_total','nadac').sum(),pl.len().alias('rx_ct'))
# .collect()
# .write_parquet('data/WV_NADAC.parquet')
#  )

def load_mc():
    mc = pl.scan_parquet(
        '/Users/matthewwine/3 Axis Advisors Dropbox/Matthew matt@3axisadvisors.com/datalake/assets/mccpdc.parquet')
    return (mc.with_columns(pl.when(c.effective_date<pl.date(2024,5,15)).then(pl.date(2022,12,31)).otherwise(c.effective_date).alias('effective_date')))

#esi = pl.scan_parquet('/Users/matthewwine/3 Axis Advisors Dropbox/Matthew matt@3axisadvisors.com/datalake/claims/ga_nadac_reports/esi*.parquet')
mc = load_mc()

def join_mc(data):
    sql = """select * from data a join mc b on a.gpi = b.gpi and a.dos between b.effective_date and b.end_date and a.is_brand = b.is_brand and a.size = b.size """
    return duckdb.sql(sql).pl().lazy()

fee = pl.when(c.is_special).then(70).otherwise(10)
carelon = pl.scan_parquet('/Users/matthewwine/3 Axis Advisors Dropbox/Matthew matt@3axisadvisors.com/datalake/claims/ga_nadac_reports/carelon*.parquet')
print(
    carelon
    .join(m.select(c.ndc,c.size,c.is_brand,c.gpi,c.product,c.gpi_10_generic_name.alias('generic_name'),c.gpi_0_category.alias('drug_class')), on='ndc')
    .filter(c.is_brand == False)
    .pipe(join_mc)
    .select(c.product,c.nadac_unit_reported,c.icp_unit,cs.contains('aff'),c.is_special,c.dos,c.unit_price,c.generic_name,c.drug_class,cs.contains('qt'))
    .with_columns(total = (c.qty*c.icp_unit), nadac = (c.nadac_unit_reported*c.qty), mc_total = (c.unit_price*c.qty)+fee)
    .group_by(['product','generic_name','drug_class',c.dos.dt.year().alias('year'),c.dos.dt.month().alias('month'),(c.mc_total < c.total).alias('is_less'),cs.contains('aff').alias('affiliated')])
    .agg(pl.col('total','mc_total','nadac').sum(),pl.len().alias('rx_ct'))
    .with_columns(pl.col(pl.Float64).cast(pl.Float32))
    .collect()
    .write_parquet('data/GA_NADAC_CARELON.parquet')
)
# print(pl.read_parquet('data/GA_NADAC.parquet').schema)
#print(pl.read_parquet('data/WV_NADAC.parquet').schema)
nadac = pl.scan_parquet('/Users/matthewwine/3 Axis Advisors Dropbox/Matthew matt@3axisadvisors.com/datalake/assets/nadac.parquet')
reprices = pl.scan_csv('/Users/matthewwine/3 Axis Advisors Dropbox/Matthew matt@3axisadvisors.com/datalake/projects/Jan_2025/MCCPDC_APP/data/mccpdc.csv', infer_schema_length=0)

def add_medispan(data):
    return data.join(m.select(c.ndc, c.size, c.is_brand, c.gpi, c.product, c.gpi_10_generic_name.alias('generic_name'),
                         c.gpi_0_category.alias('drug_class')), on='ndc')

def mc_special_series():
    return mc.filter(c.is_special).select(c.gpi).unique().collect().to_series()


def flag_mc_special(data):
    return data.with_columns(c.gpi.is_in(mc_special_series()).alias('is_special'))

# def add_nadac(data):
#     data = data.with_columns(pl.date(c.year, c.month, 1).alias('dos'))
#     sql = """
#     select a.*,
#     b.unit_price * qty nadac
#     from data a
#     left join nadac b on a.ndc=b.ndc and dos between effective_date and end_date"""
#     return duckdb.sql(sql).pl().lazy()
# # print(
#     reprices
#     .with_columns(pl.col('year').cast(pl.Int32),pl.col('month').cast(pl.Int8),pl.col('qty','current_total','mc_ic').cast(pl.Float32),c.rx_ct.cast(pl.UInt32))
#     .rename({'current_total':'total'})
#     .pipe(add_nadac)
#     .pipe(add_medispan)
#     .pipe(flag_mc_special)
#     .with_columns(mc_total = (c.mc_ic)+fee)
#     .with_columns((c.mc_total < c.total).alias('is_less'),pl.lit(False).alias('affiliated'))
#     .select((pl.scan_parquet('data/GA_NADAC.parquet').collect_schema().names()))
#     .collect()
#     .write_parquet('data/MCCPDC_REPRICES.parquet')
# )







