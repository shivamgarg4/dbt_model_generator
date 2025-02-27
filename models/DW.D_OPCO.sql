{{ config(
    materialized='incremental',
    schema='DW',
    unique_key=["OPCO_CD"],
    merge_update_columns = ['OPCO_ID', 'DATA_SRC', 'UPDATE_DT', 'UPDATE_BY', 'UPDATE_PGM', 'OPCO_DSC', 'RPT_OPCO_DSC', 'RPT_OPCO_ABBRV', 'TBA_ACTIVE_FLG']
)}}

-- Model: DW.D_OPCO
-- Source: EBI_DEV_DB.EDW.BI_D_OPCO_VW

SELECT
    OPCO_ID as OPCO_ID,
    'DW.D_OPCO' as DATA_SRC,
    CURRENT_TIMESTAMP() as CREATE_DT,
    CAST(CURRENT_USER() AS VARCHAR(200)) as CREATE_BY,
    'DW.D_OPCO' as CREATE_PGM,
    CURRENT_TIMESTAMP() as UPDATE_DT,
    CAST(CURRENT_USER() AS VARCHAR(200)) as UPDATE_BY,
    'DW.D_OPCO' as UPDATE_PGM,
    OPCO_CD as OPCO_CD,
    OPCO_DSC as OPCO_DSC,
    RPT_OPCO_DSC as RPT_OPCO_DSC,
    RPT_OPCO_ABBRV as RPT_OPCO_ABBRV,
    TBA_ACTIVE_FLG as TBA_ACTIVE_FLG
FROM {{ ref('EDW.BI_D_OPCO_VW') }} AS source
LEFT JOIN {{ ref('LND_CORE.D_ITEM') }} AS items ON source.OPCO_ID=items.OPCO_ID